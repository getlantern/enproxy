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

type idleTimingConn struct {
	conn             net.Conn
	idleTimeout      time.Duration
	lastActivityTime time.Time
	closed           chan bool
}

func newIdleTimingConn(conn net.Conn, idleTimeout time.Duration) *idleTimingConn {
	c := &idleTimingConn{
		conn:             conn,
		idleTimeout:      idleTimeout,
		lastActivityTime: time.Now(),
		closed:           make(chan bool, 10),
	}
	go func() {
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
	c.lastActivityTime = time.Now()
	return c.conn.Read(b)
}

func (c *idleTimingConn) Write(b []byte) (int, error) {
	c.lastActivityTime = time.Now()
	return c.conn.Write(b)
}

func (c *idleTimingConn) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}

func (c *idleTimingConn) Close() error {
	c.closed <- true
	return c.conn.Close()
}

func (c *idleTimingConn) closeIfNecessary() bool {
	if time.Now().Sub(c.lastActivityTime) > c.idleTimeout {
		log.Println("Closing idle conn")
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
