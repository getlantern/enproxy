package enproxy

import (
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	X_HTTPCONN_ID        = "X-HTTPConn-Id"
	X_HTTPCONN_DEST_ADDR = "X-HTTPConn-Dest-Addr"
	X_HTTPCONN_EOF       = "X-HTTPConn-EOF"
)

var (
	defaultPollInterval = 50 * time.Millisecond
	defaultIdleInterval = 5 * time.Millisecond
	defaultIdleTimeout  = 10 * time.Second
)

// idleTimingConn is a net.Conn that wraps another net.Conn and that times out
// if idle for more than idleTimeout.
type idleTimingConn struct {
	conn             net.Conn
	idleTimeout      time.Duration
	lastActivityTime time.Time
	closed           chan bool
}

// newIdleTimingConn creates a new idleTimingConn.
//
// idleTimeout specifies how long to wait for inactivity before considering
// connection idle.
//
// onClose is an optional function to call after the connection has been closed
func newIdleTimingConn(conn net.Conn, idleTimeout time.Duration, onClose func()) *idleTimingConn {
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

func OK(w io.Writer) {
	resp := &http.Response{
		StatusCode: 200,
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	resp.Write(w)
}

func BadGateway(w io.Writer, msg string) {
	log.Printf("Sending BadGateway: %s", msg)
	resp := &http.Response{
		StatusCode: 502,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Body:       &closeableStringReader{strings.NewReader(msg)},
	}
	resp.Write(w)
}

type closeableStringReader struct {
	*strings.Reader
}

func (r *closeableStringReader) Close() error {
	return nil
}
