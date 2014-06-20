package enproxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

// Connect opens a connection to the proxy and starts processing writes and
// reads to this Conn.
func (c *Conn) Connect() (err error) {
	// Generate a unique id for this connection.  This is used by the Proxy to
	// associate requests from this connection to the corresponding outbound
	// connection on the proxy side.
	c.id = uuid.NewRandom().String()

	c.initDefaults()
	c.makeChannels()
	c.markActive()

	go c.processReadsAndWrites()
	go c.postRequests()

	return
}

func (c *Conn) initDefaults() {
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
	c.nextRequestCh = make(chan *http.Request)
	c.nextResponseCh = make(chan *http.Response)
	c.nextResponseErrCh = make(chan error)
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
		case <-c.nextRequestCh:
			// ignore
		case resp := <-c.nextResponseCh:
			resp.Body.Close()
		case <-c.nextResponseErrCh:
			// ignore
		case <-c.closeCh:
			// ignore
		default:
			close(c.writeRequestsCh)
			close(c.readRequestsCh)
			close(c.nextRequestCh)
			close(c.nextResponseCh)
			close(c.nextResponseErrCh)
			close(c.closeCh)
			return
		}
	}
}

// postRequests handles writing outbound requests to the proxy and reading
// responses.  Note - this is not pipelined, because we cannot be sure that
// intervening proxies will deliver requests to the enproxy server in order.
// In-order delivery is required because we are encapsulating a stream of data
// inside the bodies of successive requests.
func (c *Conn) postRequests() {
	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, bufReader, err := c.dialProxy()
	if err != nil {
		log.Printf("Unable to dial proxy: %s", err)
		c.Close()
		return
	}
	defer func() {
		if proxyConn != nil {
			proxyConn.Close()
		}
	}()

	for {
		req, ok := <-c.nextRequestCh
		if !ok {
			// done processing requests
			return
		}

		proxyConn, bufReader, err = c.redialProxyIfNecessary(proxyConn, bufReader)
		if err != nil {
			log.Printf("Unable to redial proxy: %s", err)
			c.Close()
			return
		}
		c.lastRequestTime = time.Now()
		// Write request
		err := req.Write(proxyConn)
		if err != nil {
			c.nextResponseErrCh <- err
			return
		}

		// Read corresponding response
		resp, err := http.ReadResponse(bufReader, nil)
		if err != nil {
			c.nextResponseErrCh <- fmt.Errorf("Unable to read response from proxy: %s", err)
			return
		}

		// Check response status
		responseOK := resp.StatusCode >= 200 && resp.StatusCode < 300
		if !responseOK {
			respText := bytes.NewBuffer(nil)
			resp.Write(respText)
			c.nextResponseErrCh <- fmt.Errorf("Bad response status: %d\n%s\n", resp.StatusCode, string(respText.Bytes()))
			return
		}

		proxyHost := resp.Header.Get(X_HTTPCONN_PROXY_HOST)
		if proxyHost != "" {
			c.proxyHost = proxyHost
		}

		// Response was good, publish it
		c.nextResponseCh <- resp
	}
}

// processReadsAndWrites is this connection's run loop, which processes writes
// and reads until the Conn is closed
func (c *Conn) processReadsAndWrites() {
	defer func() {
		// We've reached the end of our run loop, which means the connection is
		// effectively closed.

		// Mark as closed, just in case
		c.markClosed()
		c.drainAndCloseChannels()
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
			// We waited more than IdleInterval for a write, proceed to reading
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
	c.bytesWritten += n

	// Let the caller know how much we wrote
	c.writeResponsesCh <- rwResponse{n, nil}
	return true
}

// initRequestToProxy sets up a new request to encapsulate our writes to Proxy
func (c *Conn) initRequestToProxy() error {
	// Construct a pipe for piping data to proxy
	c.reqBody, c.reqBodyWriter = io.Pipe()

	// Construct a new HTTP POST to encapsulate our data
	return c.post(c.reqBody)
}

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request.
func (c *Conn) processReads() (ok bool) {
	for {
		select {
		case b := <-c.readRequestsCh:
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
			// No read in IdleInterval, switch back to writing
			return true
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
		// Write an empty request to poll for any additional data
		err = c.post(nil)
		if err != nil {
			err = fmt.Errorf("Unable to write empty request: %s", err)
			return
		}
	}

	// Clear request in preparation for future writes (which will set up
	// their own request)
	c.req = nil

	// Read the next response or error
	select {
	case err = <-c.nextResponseErrCh:
	case c.resp = <-c.nextResponseCh:
	}

	return
}

func (c *Conn) dialProxy() (proxyConn net.Conn, bufReader *bufio.Reader, err error) {
	proxyConn, err = c.Config.DialProxy(c.Addr)
	if err != nil {
		err = fmt.Errorf("Unable to dial proxy: %s", err)
		return
	}
	bufReader = bufio.NewReader(proxyConn)
	return
}

// redialProxyIfNecessary redials the proxy if the original connection got
// closed somehow
func (c *Conn) redialProxyIfNecessary(origProxyConn net.Conn, origBufReader *bufio.Reader) (proxyConn net.Conn, bufReader *bufio.Reader, err error) {
	// Default to keeping the same connection
	proxyConn = origProxyConn
	bufReader = origBufReader

	// Make sure connection is still open and redial if necessary
	origProxyConn.SetReadDeadline(time.Now())
	_, err = origBufReader.Peek(1)
	origProxyConn.SetReadDeadline(time.Time{})
	if err == io.EOF {
		log.Println("Redialing proxy")
		// Close original connection
		origProxyConn.Close()
		// Dial again
		proxyConn, bufReader, err = c.dialProxy()
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		err = nil
	}
	return
}

// post posts a request with the given body to the proxy.  If writeImmediate is
// true, the request is written before post returns.  Otherwise, the request is
// written on a goroutine and post returns immediately.
func (c *Conn) post(requestBody io.ReadCloser) (err error) {
	c.req, err = c.Config.NewRequest(c.proxyHost, "POST", requestBody)
	if err != nil {
		return fmt.Errorf("Unable to construct request to proxy: %s", err)
	}
	// Send our connection id
	c.req.Header.Set(X_HTTPCONN_ID, c.id)
	// Send the address that we're trying to reach
	c.req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)

	c.nextRequestCh <- c.req
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
