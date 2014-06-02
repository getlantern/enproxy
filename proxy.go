package enproxy

import (
	"fmt"
	"io"
	"net"
	"net/http"
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
	idleInterval time.Duration

	bufferSize int

	connMap map[string]net.Conn // map of outbound connections by their id
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
func NewProxy(idleInterval time.Duration, bufferSize int) *Proxy {
	if bufferSize == 0 {
		bufferSize = DEFAULT_BUFFER_SIZE
	}
	return &Proxy{
		idleInterval: idleInterval,
		bufferSize:   bufferSize,
		connMap:      make(map[string]net.Conn),
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
	connOut, err := p.connOutFor(req)
	if err != nil {
		resp.WriteHeader(BAD_GATEWAY)
		resp.Write([]byte(fmt.Sprintf("Unable to get connOut: %s", err)))
		return
	}

	// Read request
	_, err = io.Copy(connOut, req.Body)
	if err != nil && err != io.EOF {
		resp.WriteHeader(BAD_GATEWAY)
		resp.Write([]byte(fmt.Sprintf("Unable to write to connOut: %s", err)))
		connOut.Close()
	}

	// Write response
	b := make([]byte, p.bufferSize)
	for {
		idleInterval := p.idleInterval
		if idleInterval == 0 {
			idleInterval = defaultIdleInterval
		}
		readDeadline := time.Now().Add(idleInterval)
		connOut.SetReadDeadline(readDeadline)
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
					// Return, but leave connOut open
					return
				}
			default:
				connOut.Close()
				return
			}
		}
	}
}

// connOutFor opens an outbound connection to the destination requested by the
// given http.Request.
func (p *Proxy) connOutFor(req *http.Request) (connOut net.Conn, err error) {
	id := req.Header.Get(X_HTTPCONN_ID)
	if id == "" {
		return nil, fmt.Errorf("No id found in header %s", X_HTTPCONN_ID)
	}

	connOut = p.connMap[id]
	if connOut == nil {
		// Connect to destination
		addr := req.Header.Get(X_HTTPCONN_DEST_ADDR)
		if addr == "" {
			return nil, fmt.Errorf("No address found in header %s", X_HTTPCONN_DEST_ADDR)
		}

		// Dial out on first request
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("Unable to dial out to %s: %s", addr, err)
		}

		// Wrap the connection in an idle timing one
		connOut = withIdleTimeout(conn, defaultIdleTimeout, func() {
			delete(p.connMap, id)
		})

		p.connMap[id] = connOut
	}
	return
}

// idleTimingConn is a net.Conn that wraps another net.Conn and that times out
// if idle for more than idleTimeout.
type idleTimingConn struct {
	conn             net.Conn
	idleTimeout      time.Duration
	lastActivityTime time.Time
	closed           chan bool
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
		closed:           make(chan bool, 10),
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
			case <-c.closed:
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
	c.closed <- true
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
