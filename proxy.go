package enproxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	BAD_GATEWAY         = 502
	DEFAULT_BUFFER_SIZE = 8096
)

var (
	defaultFirstReadTime      = 250 * time.Millisecond
	defaultSubsequentReadTime = 100 * time.Millisecond

	defaultFirstReadTimes = map[string]time.Duration{
		"443":  defaultIdleInterval,
		"5222": defaultIdleInterval,
	}

	defaultSubsequentReadTimes = map[string]time.Duration{
		"443":  defaultIdleInterval,
		"5222": defaultIdleInterval,
	}
)

// Proxy is the server side to an enproxy.Client.  Proxy implements the
// http.Hander interface for plugging into an HTTP server, and it also provides
// a convenience ListenAndServe() function for quickly starting up a dedicated
// HTTP server using this Proxy as its handler.
type Proxy struct {
	// Dial: function used to dial the destination server.  If nil, a default
	// TCP dialer is used.
	Dial dialFunc

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds
	IdleTimeout time.Duration

	// BufferSize: controls the size of hte buffers used for copying data from
	// outbound to inbound connections.  If given as 0, defaults to 8096 bytes.
	BufferSize int

	connMap map[string]*lazyConn // map of outbound connections by their id

	connMapMutex sync.Mutex // mutex for controlling access to connMap

	readTimingsByAddr map[string]*readTimings // profiles of read timings for sites we've accessed

	readTimingsMutex sync.Mutex // mutex controlling access to readTimingsByAddr
}

// Start() starts this proxy
func (p *Proxy) Start() {
	if p.Dial == nil {
		p.Dial = func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		}
	}
	if p.IdleTimeout == 0 {
		p.IdleTimeout = defaultIdleTimeout
	}
	if p.BufferSize == 0 {
		p.BufferSize = DEFAULT_BUFFER_SIZE
	}
	p.connMap = make(map[string]*lazyConn)
	p.readTimingsByAddr = make(map[string]*readTimings)

	go p.logReadTimings()
}

// ListenAndServe: convenience function for quickly starting up a dedicated HTTP
// server using this Proxy as its handler.
func (p *Proxy) ListenAndServe(addr string) error {
	p.Start()
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      p,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return httpServer.ListenAndServe()
}

// ServeHTTP: implements the http.Handler interface.
func (p *Proxy) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	id := req.Header.Get(X_HTTPCONN_ID)
	if id == "" {
		badGateway(resp, fmt.Sprintf("No id found in header %s", X_HTTPCONN_ID))
		return
	}

	addr := req.Header.Get(X_HTTPCONN_DEST_ADDR)
	if addr == "" {
		badGateway(resp, fmt.Sprintf("No address found in header %s", X_HTTPCONN_DEST_ADDR))
		return
	}

	connOut, err := p.getLazyConn(id, addr).get()
	if err != nil {
		badGateway(resp, fmt.Sprintf("Unable to get connOut: %s", err))
		return
	}

	// Pipe request
	_, err = io.Copy(connOut, req.Body)
	if err != nil {
		badGateway(resp, fmt.Sprintf("Unable to write to connOut: %s", err))
		connOut.Close()
		return
	}

	// Track read timings
	rt := p.getReadTimings(addr)

	// Write response
	b := make([]byte, p.BufferSize)
	first := true
	for {
		var idleTimeout time.Duration
		if first {
			idleTimeout = rt.first.estimate() * 2
		} else {
			idleTimeout = rt.subsequent.estimate() * 2
		}
		readDeadline := time.Now().Add(idleTimeout)
		connOut.SetReadDeadline(readDeadline)

		// Read
		startOfRead := time.Now()
		n, readErr := connOut.Read(b)
		// Remember read duration
		readTime := time.Now().Sub(startOfRead)
		if first {
			if readErr == io.EOF {
				// Reached EOF
				resp.Header().Set(X_HTTPCONN_EOF, "true")
			}
			// Always respond 200 OK
			resp.WriteHeader(200)
			first = false
		}
		if n > 0 {
			if first {
				rt.first.record(readTime)
			} else {
				rt.subsequent.record(readTime)
			}
		}

		// Write if necessary
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
				connOut.Close()
				return
			}
		}

		// Inspect readErr to decide whether or not to continue reading
		if readErr != nil {
			switch e := readErr.(type) {
			case net.Error:
				if e.Timeout() {
					// This means that we hit our idleInterval, which is okay
					// Return response to client, but leave connOut open
					return
				}
			default:
				// Unexpected error, close outbound connection
				connOut.Close()
				return
			}
		}
	}
}

// getLazyConn gets the lazyConn corresponding to the given id and addr
func (p *Proxy) getLazyConn(id string, addr string) (l *lazyConn) {
	p.connMapMutex.Lock()
	defer p.connMapMutex.Unlock()
	l = p.connMap[id]
	if l == nil {
		l = p.newLazyConn(id, addr)
		p.connMap[id] = l
	}
	return
}

// getReadTimings gets the readTimings for the specified addr, creating a
// new one if necessary.
func (p *Proxy) getReadTimings(addr string) *readTimings {
	p.readTimingsMutex.Lock()
	defer p.readTimingsMutex.Unlock()
	lp, found := p.readTimingsByAddr[addr]
	if !found {
		port := strings.Split(addr, ":")[1]
		first, found := defaultFirstReadTimes[port]
		if !found {
			first = defaultFirstReadTime
		}
		subsequent, found := defaultSubsequentReadTimes[port]
		if !found {
			subsequent = defaultSubsequentReadTime
		}
		lp = newReadTimings(first, subsequent)
		p.readTimingsByAddr[addr] = lp
	}
	return lp
}

func (p *Proxy) logReadTimings() {
	for {
		time.Sleep(10 * time.Second)
		for addr, readTimings := range p.readTimingsByAddr {
			log.Printf("ReadTimings for %s -- %s", addr, readTimings)
		}
	}
}

func badGateway(resp http.ResponseWriter, msg string) {
	log.Printf("Responding bad gateway: %s", msg)
	resp.Header().Set("Connection", "close")
	resp.WriteHeader(BAD_GATEWAY)
	fmt.Fprintf(resp, "No id found in header %s", X_HTTPCONN_ID)
}
