package enproxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	BAD_GATEWAY         = 502
	DEFAULT_BUFFER_SIZE = 8096
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
	for port, defaultProfile := range defaultReadTimeoutProfilesByPort {
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

	if req.Method == "POST" {
		p.handlePOST(resp, req, connOut)
	} else if req.Method == "GET" {
		p.handleGET(resp, req, connOut)
	} else {
		badGateway(resp, fmt.Sprintf("Method %s not supported", req.Method))
	}
}

// handlePOST forwards the data from a POST to the outbound connection
func (p *Proxy) handlePOST(resp http.ResponseWriter, req *http.Request, connOut net.Conn) {
	// Pipe request
	_, err := io.Copy(connOut, req.Body)
	if err != nil {
		badGateway(resp, fmt.Sprintf("Unable to write to connOut: %s", err))
		connOut.Close()
		return
	}
	resp.WriteHeader(200)
}

// handleGET streams the data from the outbound connection to the client as
// a response body.
func (p *Proxy) handleGET(resp http.ResponseWriter, req *http.Request, connOut net.Conn) {
	resp.WriteHeader(200)
	done := make(chan interface{})
	go func() {
		for {
			select {
			case <-time.After(35 * time.Millisecond):
				resp.(http.Flusher).Flush()
			case <-done:
				log.Println("handleGET() - Done")
				return
			}
		}
	}()
	io.Copy(resp, connOut)
	done <- true
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
