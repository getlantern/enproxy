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
// http.Hander interface for plugging into an HTTP server, and it also provides
// a convenience ListenAndServe() function for quickly starting up a dedicated
// HTTP server using this Proxy as its handler.
type Proxy struct {
	// Dial: function used to dial the destination server.  If nil, a default
	// TCP dialer is used.
	Dial dialFunc

	// IdleInterval: how long to wait for the next write/read before switching
	// to read/write (defaults to 15 milliseconds)
	IdleInterval time.Duration

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds
	IdleTimeout time.Duration

	// BufferSize: controls the size of hte buffers used for copying data from
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
	if p.IdleInterval == 0 {
		p.IdleInterval = defaultIdleInterval
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

	// Write response
	b := make([]byte, p.BufferSize)
	first := true
	for {
		// Only block for idleInterval on reading
		readDeadline := time.Now().Add(p.IdleInterval)
		connOut.SetReadDeadline(readDeadline)

		// Read
		n, readErr := connOut.Read(b)
		if first {
			if readErr == io.EOF {
				// Reached EOF
				resp.Header().Set(X_HTTPCONN_EOF, "true")
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
		// Get destination address and build lazy conn

		l = p.newLazyConn(id, addr)
		p.connMap[id] = l
	}
	return
}

// lazyConn is a lazily initializing conn that makes sure it is only initialized
// once.  Using these allows us to ensure that we only create one connection per
// connection id, but to still support doing the Dial calls concurrently.
type lazyConn struct {
	p           *Proxy
	id          string
	addr        string
	idleTimeout time.Duration
	connOut     net.Conn
	err         error
	mutex       sync.Mutex
}

func (p *Proxy) newLazyConn(id string, addr string) *lazyConn {
	return &lazyConn{
		p:    p,
		id:   id,
		addr: addr,
	}
}

func (l *lazyConn) get() (conn net.Conn, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.err != nil {
		// If dial already resulted in an error, return that
		return nil, err
	}
	if l.connOut == nil {
		// Lazily dial out
		conn, err := l.p.Dial(l.addr)
		if err != nil {
			l.err = fmt.Errorf("Unable to dial out to %s: %s", l.addr, err)
			return nil, l.err
		}

		// Wrap the connection in an idle timing one
		l.connOut = withIdleTimeout(conn, l.p.IdleTimeout, func() {
			delete(l.p.connMap, l.id)
		})
	}

	return l.connOut, l.err
}

func badGateway(resp http.ResponseWriter, msg string) {
	log.Printf("Responding bad gateway: %s", msg)
	resp.Header().Set("Connection", "close")
	resp.WriteHeader(BAD_GATEWAY)
	fmt.Fprintf(resp, "No id found in header %s", X_HTTPCONN_ID)
}

// idleTimingConn is a net.Conn that wraps another net.Conn and that times out
// if idle for more than idleTimeout.
type idleTimingConn struct {
	conn             net.Conn
	idleTimeout      time.Duration
	lastActivityTime time.Time
	closedCh         chan bool
}

// withIdleTimeout creates a new idleTimingConn wrapping the given net.Conn.
//
// idleTimeout specifies how long to wait for inactivity before considering
// connection idle.
//
// onClose is an optional function to call after the connection has been closed
func withIdleTimeout(conn net.Conn, idleTimeout time.Duration, onClose func()) *idleTimingConn {
	c := &idleTimingConn{
		conn:             conn,
		idleTimeout:      idleTimeout,
		lastActivityTime: time.Now(),
		closedCh:         make(chan bool, 10),
	}

	go func() {
		if onClose != nil {
			defer onClose()
		}

		for {
			select {
			case <-time.After(idleTimeout):
				if c.closeIfNecessary() {
					return
				}
			case <-c.closedCh:
				return
			}
		}
	}()

	return c
}

func (c *idleTimingConn) Read(b []byte) (int, error) {
	n, err := c.conn.Read(b)
	if n > 0 {
		c.lastActivityTime = time.Now()
	}
	return n, err
}

func (c *idleTimingConn) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	if n > 0 {
		c.lastActivityTime = time.Now()
	}
	return n, err
}

func (c *idleTimingConn) Close() error {
	c.closedCh <- true
	return c.conn.Close()
}

func (c *idleTimingConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *idleTimingConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *idleTimingConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *idleTimingConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *idleTimingConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *idleTimingConn) closeIfNecessary() bool {
	if time.Now().Sub(c.lastActivityTime) > c.idleTimeout {
		c.Close()
		return true
	}
	return false
}
