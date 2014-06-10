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
	defaultFirstReadLatency  = 250 * time.Millisecond
	defaultSecondReadLatency = 100 * time.Millisecond

	defaultFirstReadLatencies = map[string]time.Duration{
		"443":  defaultIdleInterval,
		"5222": defaultIdleInterval,
	}

	defaultSecondReadLatencies = map[string]time.Duration{
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

	latencyProfilesByAddr map[string]*latencyProfile // profiles of latency for sites we've accessed

	latencyMutex sync.Mutex // mutex controlling access to latencyProfilesByAddr
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
	p.latencyProfilesByAddr = make(map[string]*latencyProfile)
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

	// Track latencies
	lp := p.getLatencyProfile(addr)

	// Write response
	b := make([]byte, p.BufferSize)
	first := true
	for {
		var idleTimeout time.Duration
		if first {
			idleTimeout = lp.firstRead.estimate() * 2
			log.Printf("First read timeout for %s: %s", addr, idleTimeout)
		} else {
			idleTimeout = lp.secondRead.estimate() * 2
			log.Printf("Second read timeout for %s: %s", addr, idleTimeout)
		}
		readDeadline := time.Now().Add(idleTimeout)
		connOut.SetReadDeadline(readDeadline)

		// Read
		startOfRead := time.Now()
		n, readErr := connOut.Read(b)
		// Remember read duration
		latency := time.Now().Sub(startOfRead)
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
				lp.firstRead.record(latency)
			} else {
				lp.secondRead.record(latency)
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

// getLatencyProfile gets the latencyProfile for the specified addr, creating a
// new one if necessary.
func (p *Proxy) getLatencyProfile(addr string) *latencyProfile {
	p.latencyMutex.Lock()
	defer p.latencyMutex.Unlock()
	lp, found := p.latencyProfilesByAddr[addr]
	if !found {
		port := strings.Split(addr, ":")[1]
		firstReadLatency, found := defaultFirstReadLatencies[port]
		if !found {
			firstReadLatency = defaultFirstReadLatency
		}
		secondReadLatency, found := defaultSecondReadLatencies[port]
		if !found {
			secondReadLatency = defaultSecondReadLatency
		}
		lp = newLatencyProfile(firstReadLatency, secondReadLatency)
		p.latencyProfilesByAddr[addr] = lp
	}
	return lp
}

func badGateway(resp http.ResponseWriter, msg string) {
	log.Printf("Responding bad gateway: %s", msg)
	resp.Header().Set("Connection", "close")
	resp.WriteHeader(BAD_GATEWAY)
	fmt.Fprintf(resp, "No id found in header %s", X_HTTPCONN_ID)
}
