package enproxy

import (
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
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

type dialFunc func(addr string) (net.Conn, error)

// rwResponse is a response to a read or write
type rwResponse struct {
	n   int
	err error
}

// baseConn implements common functionality on client and proxy side connections
type baseConn struct {
	IdleTimeout time.Duration

	writeRequests    chan []byte      // requests to write
	writeResponses   chan rwResponse  // responses for writes
	readRequests     chan []byte      // requests to read
	readResponses    chan rwResponse  // responses for reads
	lastActivityTime time.Time        // time of last read or write
	stop             chan interface{} // stop notification
	closedMutex      sync.RWMutex     // mutex controlling access to closed flag
	closed           bool             // whether or not this Client is closed
}

func (c *baseConn) Write(b []byte) (n int, err error) {
	if c.isClosed() {
		return 0, io.EOF
	}
	c.writeRequests <- b
	res, ok := <-c.writeResponses
	if !ok {
		return 0, io.EOF
	} else {
		return res.n, res.err
	}
}

func (c *baseConn) Read(b []byte) (n int, err error) {
	if c.isClosed() {
		return 0, io.EOF
	}
	c.readRequests <- b
	res, ok := <-c.readResponses
	if !ok {
		return 0, io.EOF
	} else {
		return res.n, res.err
	}
}

func (c *baseConn) Close() error {
	if c.markClosed() {
		c.stop <- true
	}
	return nil
}

func (c *baseConn) SetDeadline(t time.Time) error {
	panic("SetDeadline not implemented")
}

func (c *baseConn) SetReadDeadline(t time.Time) error {
	panic("SetReadDeadline not implemented")
}

func (c *baseConn) SetWriteDeadline(t time.Time) error {
	panic("SetWriteDeadline not implemented")
}

func (c *baseConn) init() {
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}

	// Start clock for determining idleness
	c.lastActivityTime = time.Now()

	c.writeRequests = make(chan []byte)
	c.writeResponses = make(chan rwResponse)
	c.readRequests = make(chan []byte)
	c.readResponses = make(chan rwResponse)
	c.stop = make(chan interface{}, 100)
}

func (c *baseConn) drainAndCloseChannels() {
	c.markClosed()
	for {
		select {
		case <-c.writeRequests:
			c.writeResponses <- rwResponse{0, io.EOF}
		case <-c.readRequests:
			c.readResponses <- rwResponse{0, io.EOF}
		case <-c.stop:
			// ignore
		default:
			close(c.writeRequests)
			close(c.readRequests)
			close(c.stop)
			return
		}
	}
}

func (c *baseConn) hadActivity() {
	c.lastActivityTime = time.Now()
}

func (c *baseConn) isIdle() bool {
	timeSinceLastActivity := time.Now().Sub(c.lastActivityTime)
	return timeSinceLastActivity > c.IdleTimeout
}

func (c *baseConn) markClosed() bool {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()
	didClose := !c.closed
	c.closed = true
	return didClose
}

func (c *baseConn) isClosed() bool {
	c.closedMutex.RLock()
	defer c.closedMutex.RUnlock()
	return c.closed
}

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
