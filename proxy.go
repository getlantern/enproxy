package enproxy

import (
	"fmt"
	"io"
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
	Dial dialFunc

	idleInterval time.Duration

	bufferSize int

	connMap map[string]*lazyConn // map of outbound connections by their id

	connMapMutex sync.Mutex // mutex for controlling access to connMap
}

// NewProxy sets up a new proxy.
//
// idleInterval controls how long to wait for the next write before finishing
// the current HTTP response to the client.  If given as 0, defaults to 10
// seconds.
//
// bufferSize controls the size of the buffers used for copying data from
// outbound to inbound connection.  If given as 0, defaults to
// 8096 bytes.
//
// dial specifies the function to use to dial the destination server.  If nil,
// a default TCP dialer is used.
func NewProxy(idleInterval time.Duration, bufferSize int, dial dialFunc) *Proxy {
	if bufferSize == 0 {
		bufferSize = DEFAULT_BUFFER_SIZE
	}
	if dial == nil {
		dial = func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		}
	}
	return &Proxy{
		Dial:         dial,
		idleInterval: idleInterval,
		bufferSize:   bufferSize,
		connMap:      make(map[string]*lazyConn),
	}
}

// ListenAndServe: convenience function for quickly starting up a dedicated HTTP
// server using this Proxy as its handler.
func (p *Proxy) ListenAndServe(addr string) error {
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
		resp.WriteHeader(BAD_GATEWAY)
		fmt.Fprintf(resp, "No id found in header %s", X_HTTPCONN_ID)
		return
	}

	addr := req.Header.Get(X_HTTPCONN_DEST_ADDR)
	if addr == "" {
		resp.WriteHeader(BAD_GATEWAY)
		fmt.Fprintf(resp, "No address found in header %s", X_HTTPCONN_DEST_ADDR)
		return
	}

	connOut, err := p.getLazyConn(id, addr).get()
	if err != nil {
		resp.WriteHeader(BAD_GATEWAY)
		fmt.Fprintf(resp, "Unable to get connOut: %s", err)
		return
	}

	// Read request
	_, err = io.Copy(connOut, req.Body)
	if err != nil {
		resp.WriteHeader(BAD_GATEWAY)
		fmt.Fprintf(resp, "Unable to write to connOut: %s", err)
		connOut.Close()
		return
	}

	// Write response
	b := make([]byte, p.bufferSize)
	for {
		// Only block for idleInterval on reading
		idleInterval := p.idleInterval
		if idleInterval == 0 {
			idleInterval = defaultIdleInterval
		}
		readDeadline := time.Now().Add(idleInterval)
		connOut.SetReadDeadline(readDeadline)

		// Read
		n, readErr := connOut.Read(b)
		if readErr == io.EOF {
			// Reached EOF
			resp.Header().Set(X_HTTPCONN_EOF, "true")
		}
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
				connOut.Close()
				return
			}
		}
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
	getCh  chan interface{}
	stopCh chan interface{}
	respCh chan net.Conn
	errCh  chan error
}

func (p *Proxy) newLazyConn(id string, addr string) *lazyConn {
	l := &lazyConn{
		getCh:  make(chan interface{}),
		stopCh: make(chan interface{}),
		respCh: make(chan net.Conn),
		errCh:  make(chan error),
	}

	go func() {
		var connOut net.Conn
		var err error

		for {
			select {
			case <-l.getCh:
				if err != nil {
					// Already had an error, just return that
					l.errCh <- err
				}

				if connOut == nil {
					// Lazily connect
					conn, err := p.Dial(addr)
					if err != nil {
						err = fmt.Errorf("Unable to dial out to %s: %s", addr, err)
						l.errCh <- err
					}

					// Wrap the connection in an idle timing one
					connOut = withIdleTimeout(conn, defaultIdleTimeout, func() {
						delete(p.connMap, id)
						l.stopCh <- nil
					})
				}
				l.respCh <- connOut
			case <-l.stopCh:
				return
			}
		}
	}()

	return l
}

func (l *lazyConn) get() (conn net.Conn, err error) {
	l.getCh <- nil
	select {
	case conn = <-l.respCh:
	case err = <-l.errCh:
	}
	return
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
