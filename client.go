package httpconn

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

type newRequestFunc func(method string, body io.Reader) (*http.Request, error)

// Client is a net.Conn that tunnels its data via an httpconn.Proxy using HTTP
// requests and responses.  It assumes that streaming requests are not supported
// by the underlying servers/proxies, and so uses a polling technique similar to
// the one used by meek, but different in that data is not encoded as JSON.
// https://trac.torproject.org/projects/tor/wiki/doc/AChildsGardenOfPluggableTransports#Undertheencryption.
//
// The basics flow is as follows:
//   1. Accept writes, piping these to the proxy as the body of an http request
//   2. Continue to pipe the writes until the pause between consecutive writes
//      exceeds the IdleInterval, at which point we finish the request body
//   3. Accept reads, reading the data from the response body until EOF is
//      is reached or the gap between consecutive reads exceeds the
//      IdleInterval. If EOF wasn't reached, whenever we next accept reads, we
//      will continue to read from the same response until EOF is reached, then
//      move on to the next response.
//   4. Go back to accepting writes (step 1)
//   5. If no writes are received for more than PollInterval, issue an empty
//      request in order to pick up any new data received on the proxy, start
//      accepting reads (step 3)
//
type Client struct {
	baseConn

	// Addr: the host:port of the destination server that we're trying to reach
	Addr string

	// DialProxy: function to open a connection to the proxy
	DialProxy dialFunc

	// NewRequest: function to create a new request to the proxy
	NewRequest newRequestFunc

	// PollInterval: how frequently to poll (i.e. create a new request/response)
	// , defaults to 50 ms
	PollInterval time.Duration

	// IdleInterval: how long to wait for the next write/read before switching
	// to read/write (defaults to 1 millisecond)
	IdleInterval time.Duration

	id              string         // unique identifier for this connection
	netAddr         net.Addr       // the resolved net.Addr
	proxyConn       net.Conn       // a underlying connection to the proxy
	bufReader       *bufio.Reader  // buffered reader for proxyConn
	req             *http.Request  // the current request being used to send data
	pipeReader      *io.PipeReader // pipe reader for current request body
	pipeWriter      *io.PipeWriter // pipe writer to current request body
	resp            *http.Response // the current response being used to read data
	lastRequestTime time.Time      // time of last request
}

func (c *Client) LocalAddr() net.Addr {
	if c.proxyConn == nil {
		return nil
	} else {
		return c.proxyConn.LocalAddr()
	}
}

func (c *Client) RemoteAddr() net.Addr {
	return c.netAddr
}

// Connect opens a connection to the proxy and tells it to connect to the
// destination proxy.
func (c *Client) Connect() (err error) {
	// Resolve address
	c.netAddr, err = net.ResolveIPAddr("ip", c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to resolve address %s: %s", c.Addr, err)
	}

	// Set ID
	c.id = uuid.NewRandom().String()

	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.IdleInterval == 0 {
		c.IdleInterval = defaultIdleInterval
	}

	c.init()

	// Dial the proxy
	c.proxyConn, err = c.DialProxy(c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to dial proxy: %s", err)
	}
	c.bufReader = bufio.NewReader(c.proxyConn)

	go c.process()
	return
}

// process processes writes and reads until the Client is closed
func (c *Client) process() {
	defer func() {
		c.drainAndCloseChannels()
		c.proxyConn.Close()
	}()

	for {
		if c.isClosed() {
			return
		}
		select {
		case b := <-c.writeRequests:
			c.hadActivity()
			// Consume writes as long as they keep coming in
			if !c.processWrite(b) {
				return
			}
		case <-time.After(c.PollInterval):
			if c.isIdle() {
				// No activity for a while, stop processing
				return
			}
			// We waited more than PollInterval for a write, proceed to reading
			if !c.processReads() {
				// Problem processing reads, stop processing
				return
			}
		case <-c.stop:
			return
		}
	}
}

// processWrite processes a single write
func (c *Client) processWrite(b []byte) (ok bool) {
	if c.req == nil {
		err := c.initRequest()
		if err != nil {
			c.writeResponses <- rwResponse{0, err}
			return false
		}
	}
	n, err := c.pipeWriter.Write(b)
	if err != nil {
		c.writeResponses <- rwResponse{n, fmt.Errorf("Unable to write to proxy pipe: %s", err)}
		return false
	} else {
		c.writeResponses <- rwResponse{n, nil}
	}
	return true
}

// initRequest sets up a new request to encapsulate our writes
func (c *Client) initRequest() error {
	// Construct a pipe for piping data to proxy
	c.pipeReader, c.pipeWriter = io.Pipe()

	// Construct a new HTTP POST to encapsulate our data
	return c.post(c.pipeReader, false)
}

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or if we've hit our idle interval and still
// haven't received a read request
func (c *Client) processReads() (ok bool) {
	// Set resp to nil
	c.resp = nil
	haveReceivedReadRequest := false
	for {
		select {
		case b := <-c.readRequests:
			haveReceivedReadRequest = true
			n, err, reachedEOF := c.processRead(b)
			if n > 0 {
				c.hadActivity()
			}
			c.readResponses <- rwResponse{n, err}
			if err != nil {
				// Error doing read, stop processing
				return false
			}
			if reachedEOF {
				// Done reading the current response
				c.resp.Body.Close()
				c.resp = nil
				return true
			}
		case <-time.After(c.IdleInterval):
			if !haveReceivedReadRequest {
				// If we went IdleInterval time since switching to processReads
				// and still haven't received a read request, stop trying to
				// read.
				return true
			}
		case <-c.stop:
			c.closed = true
			return true
		}
	}
	return true
}

func (c *Client) processRead(b []byte) (n int, err error, reachedEOF bool) {
	if c.resp == nil {
		if c.req == nil {
			// Request was nil, meaning that we never wrote anything
			if time.Now().Sub(c.lastRequestTime) > c.PollInterval {
				// Write an empty request to poll for any additional incoming
				// data
				err = c.post(nil, true)
				if err != nil {
					err = fmt.Errorf("Unable to write empy request: %s", err)
					return
				}
			} else {
				// Nothing to do, return EOF
				reachedEOF = true
				return
			}
		} else {
			// Stop writing to request body
			c.pipeWriter.Close()
		}
		// Read the next response
		c.resp, err = http.ReadResponse(c.bufReader, c.req)
		if err != nil {
			err = fmt.Errorf("Unable to read response from proxy: %s", err)
			return
		}
		c.req = nil
		if c.resp.StatusCode < 200 || c.resp.StatusCode > 299 {
			err = fmt.Errorf("Error writing, response status: %d", c.resp.StatusCode)
			return
		}
		if c.resp.Header.Get(X_HTTPCONN_EOF) != "" {
			// Reached end of stream
			err = io.EOF
			reachedEOF = true
			return
		}
	}

	n, err = c.resp.Body.Read(b)
	if err == nil {
		// Do an extra read to see if we've reached EOF
		_, err = c.resp.Body.Read([]byte{})
	}
	if err == io.EOF {
		reachedEOF = true
		// Hide EOF, since it's only EOF of the current segment
		err = nil
	}
	return
}

func (c *Client) post(requestBody io.ReadCloser, writeImmediate bool) (err error) {
	c.req, err = c.NewRequest("POST", requestBody)
	if err != nil {
		return fmt.Errorf("Unable to construct request to proxy: %s", err)
	}
	// Set our ID
	c.req.Header.Set(X_HTTPCONN_ID, c.id)
	// Set the address that we're trying to reach
	c.req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)
	// c.req.Header.Set("Proxy-Connection", "Keep-Alive")

	if writeImmediate {
		c.lastRequestTime = time.Now()
		return c.req.Write(c.proxyConn)
	} else {
		// Write the request to proxy on a goroutine
		go func() {
			c.lastRequestTime = time.Now()
			err := c.req.Write(c.proxyConn)
			if err != nil {
				c.stop <- true
			}
		}()
	}
	return nil
}
