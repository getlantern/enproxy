package enproxy

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

// Connect opens a connection to the proxy and tells it to connect to the
// destination proxy.
func (c *Client) Connect() error {
	err := c.resolveAddress()
	if err != nil {
		return err
	}

	c.id = uuid.NewRandom().String()

	c.defaultTimeouts()
	c.makeChannels()
	c.startIdleTimer()

	// Dial the proxy
	err = c.dialProxy()

	go c.process()
	return nil
}

func (c *Client) resolveAddress() (err error) {
	c.netAddr, err = net.ResolveIPAddr("ip", c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to resolve address %s: %s", c.Addr, err)
	}
	return nil
}

func (c *Client) defaultTimeouts() {
	if c.Config.PollInterval == 0 {
		c.Config.PollInterval = defaultPollInterval
	}
	if c.Config.IdleInterval == 0 {
		c.Config.IdleInterval = defaultIdleInterval
	}
	if c.Config.IdleTimeout == 0 {
		c.Config.IdleTimeout = defaultIdleTimeout
	}
}

func (c *Client) makeChannels() {
	c.writeRequests = make(chan []byte)
	c.writeResponses = make(chan rwResponse)
	c.readRequests = make(chan []byte)
	c.readResponses = make(chan rwResponse)
	c.stop = make(chan interface{}, 100)
}

func (c *Client) startIdleTimer() {
	c.lastActivityTime = time.Now()
}

func (c *Client) dialProxy() (err error) {
	c.proxyConn, err = c.Config.DialProxy(c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to dial proxy: %s", err)
	}
	c.bufReader = bufio.NewReader(c.proxyConn)
	return nil
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
		case <-time.After(c.Config.PollInterval):
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
			n, err, responseFinished := c.processRead(b)
			if n > 0 {
				c.hadActivity()
			}
			c.readResponses <- rwResponse{n, err}
			if err != nil {
				// Error doing read, stop processing
				return false
			}
			if responseFinished {
				// Close response and go back to processing writes
				c.resp.Body.Close()
				c.resp = nil
				return true
			}
		case <-time.After(c.Config.IdleInterval):
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

func (c *Client) processRead(b []byte) (n int, err error, responseFinished bool) {
	if c.resp == nil {
		if c.req == nil {
			// Request was nil, meaning that we never wrote anything
			reachedPollInterval := time.Now().Sub(c.lastRequestTime) > c.Config.PollInterval
			if reachedPollInterval {
				// Write an empty request to poll for any additional data
				err = c.post(nil, true)
				if err != nil {
					err = fmt.Errorf("Unable to write empty request: %s", err)
					return
				}
			} else {
				// Nothing to do, consider response finished
				responseFinished = true
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
		// Clear request in preparation for future writes (which will set up
		// their own request)
		c.req = nil
		responseOK := c.resp.StatusCode >= 200 && c.resp.StatusCode < 300
		if !responseOK {
			err = fmt.Errorf("Error writing, response status: %d", c.resp.StatusCode)
			return
		}
	}

	n, err = c.resp.Body.Read(b)
	if err == nil {
		// Do an extra read to see if we've reached EOF
		_, err = c.resp.Body.Read([]byte{})
	}
	if err == io.EOF {
		responseFinished = true
		serverEOF := c.resp.Header.Get(X_HTTPCONN_EOF) != ""
		if !serverEOF {
			// Hide EOF, since the server thinks there may be more to read on
			// future requests
			err = nil
		}
	}
	return
}

// post posts a request with the given body to the proxy.  If writeImmediate is
// true, the request is written before post returns.  Otherwise, the request is
// written on a goroutine and post returns immediately.
func (c *Client) post(requestBody io.ReadCloser, writeImmediate bool) (err error) {
	c.req, err = c.Config.NewRequest("POST", requestBody)
	if err != nil {
		return fmt.Errorf("Unable to construct request to proxy: %s", err)
	}
	// Send our connection id
	c.req.Header.Set(X_HTTPCONN_ID, c.id)
	// Send the address that we're trying to reach
	c.req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)

	if writeImmediate {
		c.lastRequestTime = time.Now()
		return c.req.Write(c.proxyConn)
	} else {
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

// drainAndCloseChannels drains all inbound channels and then closes them, which
// ensures that any read or write ops after this connection was closed receive
// io.EOF to indicate that the connection is closed.
func (c *Client) drainAndCloseChannels() {
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

func (c *Client) hadActivity() {
	c.lastActivityTime = time.Now()
}

func (c *Client) isIdle() bool {
	timeSinceLastActivity := time.Now().Sub(c.lastActivityTime)
	return timeSinceLastActivity > c.Config.IdleTimeout
}

func (c *Client) markClosed() bool {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()
	didClose := !c.closed
	c.closed = true
	return didClose
}

func (c *Client) isClosed() bool {
	c.closedMutex.RLock()
	defer c.closedMutex.RUnlock()
	return c.closed
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
