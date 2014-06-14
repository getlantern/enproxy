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

	// FlushTimeout: how long to let reads idle before writing out a
	// response to the client.  Defaults to 35 milliseconds.
	FlushTimeout time.Duration

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds
	IdleTimeout time.Duration

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
	if p.FlushTimeout == 0 {
		p.FlushTimeout = defaultReadFlushTimeout
	}
	if p.IdleTimeout == 0 {
		p.IdleTimeout = defaultIdleTimeout
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
		p.handleGET(resp, req, lc, connOut)
	} else {
		badGateway(resp, fmt.Sprintf("Method %s not supported", req.Method))
	}
}

// handlePOST forwards the data from a POST to the outbound connection
func (p *Proxy) handlePOST(resp http.ResponseWriter, req *http.Request, connOut net.Conn) {
	// Pipe request
	_, err := io.Copy(connOut, req.Body)
	if err != nil && err != io.EOF {
		badGateway(resp, fmt.Sprintf("Unable to write to connOut: %s", err))
		return
	}
	if p.Host != "" {
		// Always feed this so clients will be guaranteed to reach
		// this particular proxy even if they originally reached us
		// through (e.g.) DNS round robin.
		resp.Header().Set(X_HTTPCONN_PROXY_HOST, p.Host)
	}
	resp.WriteHeader(200)
}

// handleGET streams the data from the outbound connection to the client as
// a response body.  If no data is read for more than FlushTimeout, then the
// response is finished and client needs to make a new GET request.
func (p *Proxy) handleGET(resp http.ResponseWriter, req *http.Request, lc *lazyConn, connOut net.Conn) {
	if lc.hitEOF {
		resp.Header().Set(X_HTTPCONN_EOF, "true")
		resp.WriteHeader(200)
		return
	}

	b := make([]byte, 64000)
	first := true
	start := time.Now()
	bytesRead := 0
	bytesInBatch := 0
	for {
		readDeadline := time.Now().Add(p.FlushTimeout)
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
				log.Printf("Write error: %s", writeErr)
				connOut.Close()
				return
			}
			bytesRead += n
			bytesInBatch += n
		}

		// Inspect readErr to decide whether or not to continue reading
		if readErr != nil {
			switch e := readErr.(type) {
			case net.Error:
				if e.Timeout() {
					// This means that we hit our idleInterval, which is okay
					// If we've read some data, return response to client, but
					// leave connOut open
					if bytesRead > 0 {
						return
					}
				}
			default:
				lc.hitEOF = true
				return
			}
		}

		if time.Now().Sub(start) > 10*time.Second {
			// We've spent 10 seconds reading, return so that CloudFlare doesn't
			// time us out
			return
		}

		if bytesInBatch > 1024768 {
			resp.(http.Flusher).Flush()
			bytesInBatch = 0
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
	resp.WriteHeader(BAD_GATEWAY)
}
