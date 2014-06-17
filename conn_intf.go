package enproxy

import (
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	X_ENPROXY_ID         = "X-Enproxy-Id"
	X_ENPROXY_DEST_ADDR  = "X-Enproxy-Dest-Addr"
	X_ENPROXY_EOF        = "X-Enproxy-EOF"
	X_ENPROXY_PROXY_HOST = "X-Enproxy-Proxy-Host"
)

var (
	defaultWriteFlushTimeout = 35 * time.Millisecond
	defaultReadFlushTimeout  = 35 * time.Millisecond
	defaultIdleTimeout       = 70 * time.Second

	// channelDepth: controls depth of processing channels.  Doesn't need to be
	// particularly big, as it's just used to prevent deadlocks in operations
	// that involve multiple channels.
	channelDepth = 100
)

// Conn is a net.Conn that tunnels its data via an httpconn.Proxy using HTTP
// requests and responses.  It assumes that streaming requests are not supported
// by the underlying servers/proxies, and so uses a polling technique similar to
// the one used by meek, but different in that data is not encoded as JSON.
// https://trac.torproject.org/projects/tor/wiki/doc/AChildsGardenOfPluggableTransports#Undertheencryption.
//
// enproxy uses two parallel channels to send and receive data.  One channel
// handles writing data out by making sequential POST requests to the server
// which encapsulate the outbound data in their request bodies, while the other
// channel handles reading data by making GET requests and grabbing the data
// encapsulated in the response bodies.
//
// Write Channel:
//
//   1. Accept writes, piping these to the proxy as the body of an http POST
//   2. Continue to pipe the writes until the pause between consecutive writes
//      exceeds the IdleInterval, at which point we finish the request body. We
//      do this because it is assumed that intervening proxies (e.g. CloudFlare
//      CDN) do not allow streaming requests, so it is necessary to finish the
//      request for data to get flushed to the destination server.
//   3. After receiving a response to the POST request, return to step 1
//
// Read Channel:
//
//   1. Accept reads, issuing a new GET request if one is not already ongoing
//   2. Process read by grabbing data from the response to the GET request
//   3. Continue to accept reads, grabbing these from the response of the
//      existing GET request
//   4. Once the response to the GET request reaches EOF, return to step 1. This
//      will happen because the proxy periodically closes responses to make sure
//      intervening proxies don't time out.
//   5. If a response is received with a special header indicating a true EOF
//      from the destination server, return EOF to the reader
//
type Conn struct {
	// Addr: the host:port of the destination server that we're trying to reach
	Addr string

	// Config: configuration of this Conn
	Config *Config

	// proxyHostCh: Self-reported FQDN of the proxy serving this connection.
	// This allows us to guarantee we reach the same server in subsequent
	// requests, even if it was initially reached through a FQDN that may
	// resolve to different IPs in different DNS lookups (e.g. as in DNS round
	// robin).
	proxyHostCh chan string

	// id: unique identifier for this connection. This is used by the Proxy to
	// associate requests from this connection to the corresponding outbound
	// connection on the Proxy side.  It is populated using a type 4 UUID.
	id string

	/* Write processing */
	writeRequestsCh  chan []byte     // requests to write
	writeResponsesCh chan rwResponse // responses for writes
	stopWriteCh      chan interface{}
	doneWriting      bool
	writeMutex       sync.RWMutex // synchronizes access to doneWriting flag

	/* Read processing */
	readRequestsCh  chan []byte     // requests to read
	readResponsesCh chan rwResponse // responses for reads
	stopReadCh      chan interface{}
	doneReading     bool
	readMutex       sync.RWMutex // synchronizes access to doneReading flag

	/* Request processing */
	requestOutCh   chan *io.PipeReader // channel for next outgoing request body
	stopRequestCh  chan interface{}
	doneRequesting bool
	requestMutex   sync.RWMutex // synchronizes access to doneRequesting flag

	/* Fields for tracking activity/closed status */
	lastActivityTime  time.Time    // time of last read or write
	lastActivityMutex sync.RWMutex // mutex controlling access to lastActivityTime
	closed            bool         // whether or not this Conn is closed
	closedMutex       sync.RWMutex // mutex controlling access to closed flag

	/* Fields for tracking current request and response */
	reqBodyWriter *io.PipeWriter // pipe writer to current request body
	resp          *http.Response // the current response being used to read data
}

// Config configures a Conn
type Config struct {
	// DialProxy: function to open a connection to the proxy
	DialProxy dialFunc

	// NewRequest: function to create a new request to the proxy
	NewRequest newRequestFunc

	// FlushTimeout: how long to let writes idle before writing out a
	// request to the proxy.  Defaults to 15 milliseconds.
	FlushTimeout time.Duration

	// IdleTimeout: how long to wait before closing an idle connection, defaults
	// to 70 seconds.  The high default value is selected to work well with XMPP
	// traffic tunneled over enproxy by Lantern.
	IdleTimeout time.Duration
}

// dialFunc is a function that dials an address (e.g. the upstream proxy)
type dialFunc func(addr string) (net.Conn, error)

// newRequestFunc is a function that builds a new request to the upstream proxy
type newRequestFunc func(host string, method string, body io.Reader) (*http.Request, error)

// rwResponse is a response to a read or write
type rwResponse struct {
	n   int
	err error
}

// Write() implements the function from net.Conn
func (c *Conn) Write(b []byte) (n int, err error) {
	if c.submitWrite(b) {
		res, ok := <-c.writeResponsesCh
		if !ok {
			return 0, io.EOF
		} else {
			return res.n, res.err
		}
	} else {
		return 0, io.EOF
	}
}

// Read() implements the function from net.Conn
func (c *Conn) Read(b []byte) (n int, err error) {
	if c.submitRead(b) {
		res, ok := <-c.readResponsesCh
		if !ok {
			return 0, io.EOF
		} else {
			return res.n, res.err
		}
	} else {
		return 0, io.EOF
	}
}

// Close() implements the function from net.Conn
func (c *Conn) Close() error {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()
	if !c.closed {
		c.stopReadCh <- nil
		c.stopWriteCh <- nil
		c.stopRequestCh <- nil
		c.closed = true
	}
	return nil
}

// isClosed checks whether or not this connection is closed
func (c *Conn) isClosed() bool {
	c.closedMutex.RLock()
	defer c.closedMutex.RUnlock()
	return c.closed
}

// LocalAddr() is not implemented
func (c *Conn) LocalAddr() net.Addr {
	panic("LocalAddr() not implemented")
}

// RemoteAddr() is not implemented
func (c *Conn) RemoteAddr() net.Addr {
	panic("RemoteAddr() not implemented")
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
