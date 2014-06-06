package enproxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

// Connect opens a connection to the proxy and starts processing writes and
// reads to this Conn.
func (c *Conn) Connect() error {
	// Generate a unique id for this connection.  This is used by the Proxy to
	// associate requests from this connection to the corresponding outbound
	// connection on the proxy side.
	c.id = uuid.NewRandom().String()

	c.initDefaults()
	c.makeChannels()
	c.markActive()

	// Dial the proxy
	err := c.dialProxy()
	if err != nil {
		return err
	}

	// Start our run loop
	go c.process()
	return nil
}

func (c *Conn) initDefaults() {
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

// makeChannels makes all the channels that we need for processing read, write
// and close requests on this connection.
func (c *Conn) makeChannels() {
	c.writeRequestsCh = make(chan []byte)
	c.writeResponsesCh = make(chan rwResponse)
	c.readRequestsCh = make(chan []byte)
	c.readResponsesCh = make(chan rwResponse)
	c.closeCh = make(chan interface{})
}

// drainAndCloseChannels drains all inbound channels and then closes them, which
// ensures that any read or write ops submitted after this connection was closed
// receive io.EOF to indicate that the connection is closed.
func (c *Conn) drainAndCloseChannels() {
	for {
		select {
		case <-c.writeRequestsCh:
			c.writeResponsesCh <- rwResponse{0, io.EOF}
		case <-c.readRequestsCh:
			c.readResponsesCh <- rwResponse{0, io.EOF}
		case <-c.closeCh:
			// ignore
		default:
			close(c.writeRequestsCh)
			close(c.readRequestsCh)
			close(c.closeCh)
			return
		}
	}
}

func (c *Conn) dialProxy() (err error) {
	c.proxyConn, err = c.Config.DialProxy(c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to dial proxy: %s", err)
	}
	c.bufReader = bufio.NewReader(c.proxyConn)
	return nil
}

// process is this connection's run loop, which processes writes and reads until
// the Conn is closed
func (c *Conn) process() {
	defer func() {
		// We've reached the end of our run loop, which means the connection is
		// effectively closed.

		// Mark as closed, just in case
		c.markClosed()
		c.drainAndCloseChannels()
		c.proxyConn.Close()
	}()

	for {
		if c.isClosed() {
			return
		}
		select {
		case b := <-c.writeRequestsCh:
			c.markActive()
			// Consume writes as long as they keep coming in
			if !c.processWrite(b) {
				// Problem writing, stop processing
				return
			}
		case <-time.After(c.Config.IdleInterval):
			if c.isIdle() {
				// No activity for more than IdleTimeout, stop processing
				return
			}
			// We waited more than PollInterval for a write, proceed to reading
			if !c.processReads() {
				// Problem processing reads, stop processing
				return
			}
		case <-c.closeCh:
			// Stop requested, stop processing
			return
		}
	}
}

// processWrite processes a single write, returning true if the write was
// successful, false otherwise.
func (c *Conn) processWrite(b []byte) (ok bool) {
	if c.req == nil {
		// Lazily initialize our next request to the proxy
		err := c.initRequestToProxy()
		if err != nil {
			c.writeResponsesCh <- rwResponse{0, err}
			return false
		}
	}

	// Write out data to the request body
	n, err := c.reqBodyWriter.Write(b)
	if err != nil {
		c.writeResponsesCh <- rwResponse{n, fmt.Errorf("Unable to write to proxy pipe: %s", err)}
		return false
	}

	// Let the caller know how much we wrote
	c.writeResponsesCh <- rwResponse{n, nil}
	return true
}

// initRequestToProxy sets up a new request to encapsulate our writes to Proxy
func (c *Conn) initRequestToProxy() error {
	// Construct a pipe for piping data to proxy
	c.reqBody, c.reqBodyWriter = io.Pipe()

	// Construct a new HTTP POST to encapsulate our data
	return c.post(c.reqBody, ASYNCHRONOUS)
}

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request.
func (c *Conn) processReads() (ok bool) {
	haveReceivedReadRequest := false
	for {
		select {
		case b := <-c.readRequestsCh:
			haveReceivedReadRequest = true
			n, err, responseFinished := c.processRead(b)
			if n > 0 {
				c.markActive()
			}
			c.readResponsesCh <- rwResponse{n, err}
			if err != nil {
				// Error doing read, stop processing
				return false
			}
			if responseFinished {
				if c.resp != nil {
					// Close response and go back to processing writes
					c.resp.Body.Close()
					c.resp = nil
				}
				return true
			}
		case <-time.After(c.Config.IdleInterval):
			if !haveReceivedReadRequest {
				// If we went IdleInterval time since switching to processReads
				// and still haven't received a read request, stop trying to
				// read.
				return true
			}
		case <-c.closeCh:
			c.closed = true
			return true
		}
	}
	return true
}

// processRead processes a single read
func (c *Conn) processRead(b []byte) (n int, err error, responseFinished bool) {
	if c.resp == nil {
		err, responseFinished = c.readNextResponse()
		if err != nil || responseFinished {
			return
		}
	}

	// Read from response body
	if c.resp.Body == nil {
		// No response body, treat like EOF
		err = io.EOF
	} else {
		n, err = c.resp.Body.Read(b)
		if err == nil {
			// Read again, just in case we missed EOF the first time around.
			// The reason for this is that per the io.Reader interface, when
			// Read() reaches EOF, it is allowed to return the number of bytes
			// read and a nil error.  Subsequent calls to Read however must
			// return EOF, so making the additional read
			_, err = c.resp.Body.Read(emptyBuffer)
		}
	}
	if err == io.EOF {
		// We've reached EOF on this response body (not on the connection)
		responseFinished = true
		proxyEOF := c.resp.Header.Get(X_HTTPCONN_EOF) != ""
		if !proxyEOF {
			// Just because we've reached EOF on this response body doesn't mean
			// that destination server doesn't have more data to send via the
			// proxy. Once the destination server has reached EOF, the proxy
			// will let us know by setting X_HTTPCONN_EOF to true.
			err = nil
		}
	}
	return
}

func (c *Conn) readNextResponse() (err error, responseFinished bool) {
	if c.req != nil {
		// Stop writing to request body to stop current request to server
		c.reqBodyWriter.Close()
	} else {
		// Request was nil, meaning that we never wrote anything
		reachedPollInterval := time.Now().Sub(c.lastRequestTime) > c.Config.PollInterval
		if reachedPollInterval {
			// Write an empty request to poll for any additional data
			err = c.post(nil, IMMEDIATE)
			if err != nil {
				err = fmt.Errorf("Unable to write empty request: %s", err)
				return
			}
		} else {
			// Nothing written, haven't hit poll interval yet, nothing to do
			responseFinished = true
			return
		}
	}

	// Clear request in preparation for future writes (which will set up
	// their own request)
	c.req = nil

	// Read the next response
	c.resp, err = http.ReadResponse(c.bufReader, nil)
	if err != nil {
		err = fmt.Errorf("Unable to read response from proxy: %s", err)
		return
	}

	// Check response status
	responseOK := c.resp.StatusCode >= 200 && c.resp.StatusCode < 300
	if !responseOK {
		respText := bytes.NewBuffer(nil)
		c.resp.Write(respText)
		err = fmt.Errorf("Error writing, response status: %d\n\n", c.resp.StatusCode, string(respText.Bytes()))
		return
	}

	return
}

// post posts a request with the given body to the proxy.  If writeImmediate is
// true, the request is written before post returns.  Otherwise, the request is
// written on a goroutine and post returns immediately.
func (c *Conn) post(requestBody io.ReadCloser, writeImmediate bool) (err error) {
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
			if err != nil && !c.isClosed() {
				c.closeCh <- true
			}
		}()
	}
	return nil
}

func (c *Conn) markActive() {
	c.lastActivityTime = time.Now()
}

func (c *Conn) isIdle() bool {
	timeSinceLastActivity := time.Now().Sub(c.lastActivityTime)
	return timeSinceLastActivity > c.Config.IdleTimeout
}

func (c *Conn) markClosed() bool {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()
	didClose := !c.closed
	c.closed = true
	return didClose
}

func (c *Conn) isClosed() bool {
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
