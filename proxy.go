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
	BAD_GATEWAY = 502

	DEFAULT_BYTES_BEFORE_FLUSH = 1024768
	DEFAULT_READ_BUFFER_SIZE   = 65536
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

	// BytesBeforeFlush: how many bytes to read before flushing response to
	// client.  Periodically flushing the response keeps the response buffer
	// from getting too big when processing big downloads.
	BytesBeforeFlush int

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds
	IdleTimeout time.Duration

	// ReadBufferSize: size of read buffer in bytes
	ReadBufferSize int

	// connMap: map of outbound connections by their id
	connMap map[string]*lazyConn

	// connMapMutex: synchronizes access to connMap
	connMapMutex sync.Mutex
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
		p.IdleTimeout = defaultIdleTimeoutServer
	}
	if p.ReadBufferSize == 0 {
		p.ReadBufferSize = DEFAULT_READ_BUFFER_SIZE
	}
	if p.BytesBeforeFlush == 0 {
		p.BytesBeforeFlush = DEFAULT_BYTES_BEFORE_FLUSH
	}
	p.connMap = make(map[string]*lazyConn)
}

// ListenAndServe: convenience function for quickly starting up a dedicated HTTP
// server using this Proxy as its handler
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

// ServeHTTP: implements the http.Handler interface
func (p *Proxy) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	id := req.Header.Get(X_ENPROXY_ID)
	if id == "" {
		badGateway(resp, fmt.Sprintf("No id found in header %s", X_ENPROXY_ID))
		return
	}

	addr := req.Header.Get(X_ENPROXY_DEST_ADDR)
	if addr == "" {
		badGateway(resp, fmt.Sprintf("No address found in header %s", X_ENPROXY_DEST_ADDR))
		return
	}

	lc, isNew := p.getLazyConn(id, addr)
	connOut, err := lc.get()
	if err != nil {
		badGateway(resp, fmt.Sprintf("Unable to get connOut: %s", err))
		return
	}

	op := req.Header.Get(X_ENPROXY_OP)
	if op == OP_WRITE {
		p.handleWrite(resp, req, lc, connOut, isNew)
	} else if op == OP_READ {
		p.handleRead(resp, req, lc, connOut, true)
	} else {
		badGateway(resp, fmt.Sprintf("Op %s not supported", op))
	}
}

// handleWrite forwards the data from a POST to the outbound connection
func (p *Proxy) handleWrite(resp http.ResponseWriter, req *http.Request, lc *lazyConn, connOut net.Conn, first bool) {
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
		resp.Header().Set(X_ENPROXY_PROXY_HOST, p.Host)
	}
	if first {
		// On first write, immediately do some reading
		p.handleRead(resp, req, lc, connOut, false)
	} else {
		resp.WriteHeader(200)
	}
}

// handleRead streams the data from the outbound connection to the client as
// a response body.  If no data is read for more than FlushTimeout, then the
// response is finished and client needs to make a new GET request.
func (p *Proxy) handleRead(resp http.ResponseWriter, req *http.Request, lc *lazyConn, connOut net.Conn, waitForData bool) {
	if lc.hitEOF {
		// We hit EOF on the server while processing a previous request,
		// immediately return EOF to the client
		resp.Header().Set(X_ENPROXY_EOF, "true")
		resp.WriteHeader(200)
		return
	}

	b := make([]byte, p.ReadBufferSize)
	first := true
	start := time.Now()
	haveRead := false
	bytesInBatch := 0
	for {
		readDeadline := time.Now().Add(p.FlushTimeout)
		connOut.SetReadDeadline(readDeadline)

		// Read
		n, readErr := connOut.Read(b)
		if first {
			if readErr == io.EOF {
				// Reached EOF, tell client using a special header
				resp.Header().Set(X_ENPROXY_EOF, "true")
			}
			// Always respond 200 OK
			resp.WriteHeader(200)
			first = false
		}

		// Write if necessary
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
				log.Printf("Error writing to response: %s", writeErr)
				connOut.Close()
				return
			}
			bytesInBatch = bytesInBatch + n
			haveRead = true
		}

		// Inspect readErr to decide whether or not to continue reading
		if readErr != nil {
			switch e := readErr.(type) {
			case net.Error:
				if e.Timeout() && n == 0 {
					if n == 0 {
						if !waitForData || haveRead {
							return
						}
					}

					// If we didn't read yet, keep response open
				}
			default:
				if readErr == io.EOF {
					lc.hitEOF = true
				} else {
					log.Printf("Unexpected error reading from upstream: %s", readErr)
				}
				return
			}
		}

		if time.Now().Sub(start) > 10*time.Second {
			// We've spent more than 10 seconds reading, return so that
			// CloudFlare doesn't time us out
			return
		}

		if bytesInBatch > p.BytesBeforeFlush {
			// We've read a good chunk, flush the response to keep its buffer
			// from getting too big.
			resp.(http.Flusher).Flush()
			bytesInBatch = 0
		}
	}
}

// getLazyConn gets the lazyConn corresponding to the given id and addr, or
// creates a new one and saves it to connMap.
func (p *Proxy) getLazyConn(id string, addr string) (l *lazyConn, isNew bool) {
	p.connMapMutex.Lock()
	defer p.connMapMutex.Unlock()
	l = p.connMap[id]
	if l == nil {
		l = p.newLazyConn(id, addr)
		p.connMap[id] = l
		isNew = true
	}
	return
}

func badGateway(resp http.ResponseWriter, msg string) {
	resp.WriteHeader(BAD_GATEWAY)
}
