package enproxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	X_HTTPCONN_ID        = "X-HTTPConn-Id"
	X_HTTPCONN_DEST_ADDR = "X-HTTPConn-Dest-Addr"
	X_HTTPCONN_EOF       = "X-HTTPConn-EOF"

	IMMEDIATE    = true
	ASYNCHRONOUS = false
)

var (
	defaultPollInterval = 25 * time.Millisecond
	defaultIdleInterval = 25 * time.Millisecond
	defaultIdleTimeout  = 70 * time.Second

	emptyBuffer = []byte{}
)

// Conn is a net.Conn that tunnels its data via an httpconn.Proxy using HTTP
// requests and responses.  It assumes that streaming requests are not supported
// by the underlying servers/proxies, and so uses a polling technique similar to
// the one used by meek, but different in that data is not encoded as JSON.
// https://trac.torproject.org/projects/tor/wiki/doc/AChildsGardenOfPluggableTransports#Undertheencryption.
//
// enproxy doesn't support request pipelining, so new requests are only sent
// after previous responses have been read.
//
// The basics flow is as follows:
//   1. Accept writes, piping these to the proxy as the body of an http request
//   2. Continue to pipe the writes until the pause between consecutive writes
//      exceeds the IdleInterval, at which point we finish the request body
//   3. Accept reads, reading the data from the response body until EOF is
//      is reached or no read was attempted for more than IdleInterval.
//   4. Go back to accepting writes (step 1)
//   5. If no writes are received for more than PollInterval, issue an empty
//      request in order to pick up any new data received by the proxy, start
//      accepting reads (step 3)
//
type Conn struct {
	// Addr: the host:port of the destination server that we're trying to reach
	Addr string

	// Config: configuration of this Conn
	Config *Config

	id string // unique identifier for this connection

	/* Channels for processing reads, writes and closes */
	writeRequestsCh  chan []byte      // requests to write
	writeResponsesCh chan rwResponse  // responses for writes
	readRequestsCh   chan []byte      // requests to read
	readResponsesCh  chan rwResponse  // responses for reads
	closeCh          chan interface{} // close notification

	/* Fields for tracking activity/closed status */
	lastActivityTime time.Time    // time of last read or write
	closedMutex      sync.RWMutex // mutex controlling access to closed flag
	closed           bool         // whether or not this Conn is closed

	/* Networking stuff */
	proxyConn net.Conn      // the connection to the proxy
	bufReader *bufio.Reader // buffered reader for proxyConn

	/* Fields for tracking current request and response */
	req             *http.Request  // the current request being used to send data
	reqBody         *io.PipeReader // pipe reader for current request body
	reqBodyWriter   *io.PipeWriter // pipe writer to current request body
	resp            *http.Response // the current response being used to read data
	lastRequestTime time.Time      // time of last request
}

type dialFunc func(addr string) (net.Conn, error)

type newRequestFunc func(method string, body io.Reader) (*http.Request, error)

// rwResponse is a response to a read or write
type rwResponse struct {
	n   int
	err error
}

// Config configures a Conn
type Config struct {
	// DialProxy: function to open a connection to the proxy
	DialProxy dialFunc

	// NewRequest: function to create a new request to the proxy
	NewRequest newRequestFunc

	// IdleTimeout: how long to wait for a read before switching to writing,
	// defaults to 70 seconds
	IdleTimeout time.Duration

	// PollInterval: how frequently to poll (i.e. create a new request/response)
	// , defaults to 50 ms
	PollInterval time.Duration

	// IdleInterval: how long to wait for the next write/read before switching
	// to read/write (defaults to 5 milliseconds)
	IdleInterval time.Duration
}

// LocalAddr() implements the function from net.Conn
func (c *Conn) LocalAddr() net.Addr {
	if c.proxyConn == nil {
		return nil
	} else {
		return c.proxyConn.LocalAddr()
	}
}

// RemoteAddr() is not implemented
func (c *Conn) RemoteAddr() net.Addr {
	panic("RemoteAddr() not implemented")
}

// Write() implements the function from net.Conn
func (c *Conn) Write(b []byte) (n int, err error) {
	if c.isClosed() {
		return 0, io.EOF
	}
	c.writeRequestsCh <- b
	res, ok := <-c.writeResponsesCh
	if !ok {
		return 0, io.EOF
	} else {
		return res.n, res.err
	}
}

// Read() implements the function from net.Conn
func (c *Conn) Read(b []byte) (n int, err error) {
	if c.isClosed() {
		return 0, io.EOF
	}
	c.readRequestsCh <- b
	res, ok := <-c.readResponsesCh
	if !ok {
		return 0, io.EOF
	} else {
		return res.n, res.err
	}
}

// Close() implements the function from net.Conn
func (c *Conn) Close() error {
	if c.markClosed() {
		c.closeCh <- true
	}
	return nil
}

// SetDeadline() is currently unimplemented.
func (c *Conn) SetDeadline(t time.Time) error {
	panic("SetDeadline not implemented")
}

// SetReadDeadline() is currently unimplemented.
func (c *Conn) SetReadDeadline(t time.Time) error {
	panic("SetReadDeadline not implemented")
}

// SetWriteDeadline() is currently unimplemented.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	panic("SetWriteDeadline not implemented")
}
