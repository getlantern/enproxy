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
	shortTimeout    = 25 * time.Millisecond
	mediumTimeout   = 250 * time.Millisecond
	longTimeout     = 1000 * time.Millisecond
	largeFileCutoff = 50000

	// defaultTimeoutProfile is optimized for low latency
	defaultTimoutProfile = NewTimeoutProfile(shortTimeout)

	// defaultTimeoutProfilesByPort
	//
	// Timeout profiles are determined based on the following heuristic:
	//
	// HTTP - typically the latency to first response on an HTTP request will be
	//        high because the server has to prepare/find and then return the
	//        content.  Once the content starts streaming, reads should proceed
	//        relatively quickly.  If we're reading a lot of data (say more than
	//        50Kb, then we're possibly looking at a large file download, which
	//        we want to make sure streams completely in one response, so we set
	//        a bigger read timeout)
	//
	// HTTPS - the initial traffic is all handshaking, which needs to proceed
	//         with as low latency as possible, so we use a short timeout.  If
	//         we enter into a large file scenario, then we bump up the timeout
	//         to provide more complete streaming responses.
	//
	defaultTimeoutProfilesByPort = map[string]*TimeoutProfile{
		"80":  NewTimeoutProfile(longTimeout).WithTimeoutAfter(1, shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout),
		"443": NewTimeoutProfile(shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout),
	}
)

// Proxy is the server side to an enproxy.Client.  Proxy implements the
// http.Handler interface for plugging into an HTTP server, and it also
// provides a convenience ListenAndServe() function for quickly starting up
// a dedicated HTTP server using this Proxy as its handler.
type Proxy struct {
	// Dial: function used to dial the destination server.  If nil, a default
	// TCP dialer is used.
	Dial dialFunc

	// Host: FQDN that is guaranteed to hit this particular proxy.  Required
	// if this server was originally reached by e.g. DNS round robin.
	Host string

	// DefaultTimeoutProfile: default profile determining read timeouts based on
	// bytes read
	DefaultTimeoutProfile *TimeoutProfile

	// TimeoutProfilesByPort: profiles determining read timeouts based on bytes
	// read, with a different profile by port
	TimeoutProfilesByPort map[string]*TimeoutProfile

	// IdleInterval: time to wait for next read before returning response.
	// Defaults to 25ms.
	// Applies to all traffic except port 80 (see IdleIntervalHTTP).
	IdleInterval time.Duration

	// IdleIntervalHTTP: like IdleInterval, but only for traffic on port 80.
	// We use a larger IdleInterval for HTTP since it doesn't involve a chatty
	// handshake at the beginning and is typically
	IdleIntervalHTTP time.Duration

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds
	IdleTimeout time.Duration

	// BufferSize: controls the size of the buffers used for copying data from
	// outbound to inbound connections.  If given as 0, defaults to 8096 bytes.
	BufferSize int

	connMap map[string]*lazyConn // map of outbound connections by their id

	connMapMutex sync.Mutex // mutex for controlling access to connMap
}

// Start() starts this proxy
func (p *Proxy) Start() {
	if p.Dial == nil {
		p.Dial = func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		}
	}
	if p.DefaultTimeoutProfile == nil {
		p.DefaultTimeoutProfile = defaultTimoutProfile
	}
	if p.TimeoutProfilesByPort == nil {
		p.TimeoutProfilesByPort = make(map[string]*TimeoutProfile)
	}
	for port, defaultProfile := range defaultTimeoutProfilesByPort {
		_, exists := p.TimeoutProfilesByPort[port]
		if !exists {
			// Merge default into map
			p.TimeoutProfilesByPort[port] = defaultProfile
		}
	}
	if p.IdleTimeout == 0 {
		p.IdleTimeout = defaultIdleTimeout
	}
	if p.BufferSize == 0 {
		p.BufferSize = DEFAULT_BUFFER_SIZE
	}
	p.connMap = make(map[string]*lazyConn)
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

	lc := p.getLazyConn(id, addr)
	connOut, err := lc.get()
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

	// Get timeout profile based on addr
	port := strings.Split(addr, ":")[1]
	tp := p.TimeoutProfilesByPort[port]
	if tp == nil {
		tp = p.DefaultTimeoutProfile
	}

	// Write response
	b := make([]byte, p.BufferSize)
	first := true
	for {
		timeout := tp.GetTimeoutAfter(lc.bytesRead)
		readDeadline := time.Now().Add(timeout)
		connOut.SetReadDeadline(readDeadline)

		// Read
		n, readErr := connOut.Read(b)
		if first {
			if readErr == io.EOF {
				// Reached EOF
				resp.Header().Set(X_HTTPCONN_EOF, "true")
			}
			if p.Host != "" {
				// Always feed this so clients will be guaranteed to reach
				// this particular proxy even if they originally reached us
				// through (e.g.) DNS round robin.
				resp.Header().Set(X_HTTPCONN_PROXY_HOST, p.Host)
			}
			// Always respond 200 OK
			resp.WriteHeader(200)
			first = false
		}

		// Write if necessary
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
				connOut.Close()
				return
			}
			lc.bytesRead += n
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

func badGateway(resp http.ResponseWriter, msg string) {
	log.Printf("Responding bad gateway: %s", msg)
	resp.Header().Set("Connection", "close")
	resp.WriteHeader(BAD_GATEWAY)
	fmt.Fprintf(resp, "No id found in header %s", X_HTTPCONN_ID)
}
