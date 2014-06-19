package enproxy

import (
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	X_HTTPCONN_ID         = "X-HTTPConn-Id"
	X_HTTPCONN_DEST_ADDR  = "X-HTTPConn-Dest-Addr"
	X_HTTPCONN_EOF        = "X-HTTPConn-EOF"
	X_HTTPCONN_PROXY_HOST = "X-HTTPConn-Proxy-Host"
)

var (
	defaultPollInterval = 50 * time.Millisecond
	defaultIdleTimeout  = 70 * time.Second
	// 70 seconds seems to play nicely with Chrome. Setting defaultIdleTimeout
	// too low seems to cause a lot of ERR_CONNECTION_CLOSED errors in Chrome.
	// We still get these with a larger idle timeout, but a lot less frequently.

	shortTimeout          = 35 * time.Millisecond
	mediumTimeout         = 350 * time.Millisecond
	longTimeout           = 1000 * time.Millisecond
	largeFileCutoff       = 50000
	reallyLargeFileCutoff = 250000

	// defaultTimeoutProfile is optimized for low latency
	defaultTimoutProfile = NewTimeoutProfile(shortTimeout)

	// defaultReadTimeoutProfilesByPort
	//
	// Read timeout profiles are determined based on the following heuristic:
	//
	// HTTP - typically the latency to first response on an HTTP request will be
	//        high because the server has to prepare/find and then return the
	//        content.  Once the content starts streaming, reads should proceed
	//        relatively quickly.  If we're reading a lot of data (say more than
	//        50Kb, then we're possibly looking at a large file download, which
	//        we want to make sure streams completely in one response, so we set
	//        a bigger read timeout)
	//
	// HTTPS - the initial traffic is all handshaking, which needs to proceed
	//         with as low latency as possible, so we use a short timeout.  If
	//         we enter into a large file scenario, then we bump up the timeout
	//         to provide more complete streaming responses.
	//
	defaultReadTimeoutProfilesByPort = map[string]*TimeoutProfile{
		"80":  NewTimeoutProfile(longTimeout).WithTimeoutAfter(1, shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout).WithTimeoutAfter(reallyLargeFileCutoff, longTimeout),
		"443": NewTimeoutProfile(shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout).WithTimeoutAfter(reallyLargeFileCutoff, longTimeout),
	}

	defaultWriteTimeoutProfilesByPort = map[string]*TimeoutProfile{
		"80":  NewTimeoutProfile(shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout).WithTimeoutAfter(reallyLargeFileCutoff, longTimeout),
		"443": NewTimeoutProfile(shortTimeout).WithTimeoutAfter(largeFileCutoff, mediumTimeout).WithTimeoutAfter(reallyLargeFileCutoff, longTimeout),
	}

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

	// Self-reported FQDN of the proxy serving this connection.  This allows
	// us to guarantee we reach the same server in subsequent requests, even
	// if it was initially reached through a FQDN that may resolve to
	// different IPs in different DNS lookups (e.g. as in DNS round robin).
	proxyHost string

	id string // unique identifier for this connection

	/* Channels for processing reads, writes and closes */
	writeRequestsCh   chan []byte         // requests to write
	writeResponsesCh  chan rwResponse     // responses for writes
	readRequestsCh    chan []byte         // requests to read
	readResponsesCh   chan rwResponse     // responses for reads
	nextRequestCh     chan *http.Request  // channel for next outgoing request
	nextResponseCh    chan *http.Response // channel for next response
	nextResponseErrCh chan error          // channel for error on next response
	closeCh           chan interface{}    // close notification

	/* Fields for tracking activity/closed status */
	bytesWritten     int
	lastActivityTime time.Time    // time of last read or write
	closedMutex      sync.RWMutex // mutex controlling access to closed flag
	closed           bool         // whether or not this Conn is closed

	/* Fields for tracking current request and response */
	req             *http.Request  // the current request being used to send data
	reqBody         *io.PipeReader // pipe reader for current request body
	reqBodyWriter   *io.PipeWriter // pipe writer to current request body
	resp            *http.Response // the current response being used to read data
	lastRequestTime time.Time      // time of last request
}

type dialFunc func(addr string) (net.Conn, error)

type newRequestFunc func(host string, method string, body io.Reader) (*http.Request, error)

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

	// DefaultTimeoutProfile: default profile determining write timeouts based on
	// bytes read
	DefaultTimeoutProfile *TimeoutProfile

	// TimeoutProfilesByPort: profiles determining write timeouts based on bytes
	// read, with a different profile by port
	TimeoutProfilesByPort map[string]*TimeoutProfile

	// PollInterval: how frequently to poll (i.e. create a new request/response)
	// , defaults to 50 ms
	PollInterval time.Duration

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds.  The high default value is selected to work well with XMPP
	// traffic tunneled over enproxy by Lantern.
	IdleTimeout time.Duration
}

// LocalAddr() is not implemented
func (c *Conn) LocalAddr() net.Addr {
	panic("LocalAddr() not implemented")
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
