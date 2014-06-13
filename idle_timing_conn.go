package enproxy

import (
	"net"
	"time"
)

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
		c.markActive()
	}
	return n, err
}

func (c *idleTimingConn) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	if n > 0 {
		c.markActive()
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

func (c *idleTimingConn) markActive() {
	c.lastActivityTime = time.Now()
}

func (c *idleTimingConn) closeIfNecessary() bool {
	if time.Now().Sub(c.lastActivityTime) > c.idleTimeout {
		c.Close()
		return true
	}
	return false
}
