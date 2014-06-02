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
func (c *Conn) Connect() error {
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
	if err != nil {
		return err
	}

	go c.process()
	return nil
}

func (c *Conn) resolveAddress() (err error) {
	c.netAddr, err = net.ResolveIPAddr("ip", strings.Split(c.Addr, ":")[0])
	if err != nil {
		return fmt.Errorf("Unable to resolve address %s: %s", c.Addr, err)
	}
	return nil
}

func (c *Conn) defaultTimeouts() {
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

func (c *Conn) makeChannels() {
	c.writeRequests = make(chan []byte)
	c.writeResponses = make(chan rwResponse)
	c.readRequests = make(chan []byte)
	c.readResponses = make(chan rwResponse)
	c.stop = make(chan interface{}, 100)
}

func (c *Conn) startIdleTimer() {
	c.lastActivityTime = time.Now()
}

func (c *Conn) dialProxy() (err error) {
	c.proxyConn, err = c.Config.DialProxy(c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to dial proxy: %s", err)
	}
	c.bufReader = bufio.NewReader(c.proxyConn)
	return nil
}

// process processes writes and reads until the Conn is closed
func (c *Conn) process() {
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
			c.markActive()
			// Consume writes as long as they keep coming in
			if !c.processWrite(b) {
				return
			}
		case <-time.After(c.Config.PollInterval):
			if c.isIdle() {
				// No activity for more than IdleTimeout, stop processing
				return
			}
			// We waited more than PollInterval for a write, proceed to reading
			if !c.processReads() {
				// Problem processing reads, stop processing
				return
			}
		case <-c.stop:
			// Stop requested, stop processing
			return
		}
	}
}

// processWrite processes a single write
func (c *Conn) processWrite(b []byte) (ok bool) {
	if c.req == nil {
		err := c.initRequestToProxy()
		if err != nil {
			c.writeResponses <- rwResponse{0, err}
			return false
		}
	}
	n, err := c.reqBodyWriter.Write(b)
	if err != nil {
		c.writeResponses <- rwResponse{n, fmt.Errorf("Unable to write to proxy pipe: %s", err)}
		return false
	}
	c.writeResponses <- rwResponse{n, nil}
	return true
}

// initRequestToProxy sets up a new request to encapsulate our writes to Proxy
func (c *Conn) initRequestToProxy() error {
	// Construct a pipe for piping data to proxy
	c.reqBody, c.reqBodyWriter = io.Pipe()

	// Construct a new HTTP POST to encapsulate our data
	return c.post(c.reqBody, false)
}

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request
func (c *Conn) processReads() (ok bool) {
	haveReceivedReadRequest := false
	for {
		select {
		case b := <-c.readRequests:
			haveReceivedReadRequest = true
			n, err, responseFinished := c.processRead(b)
			if n > 0 {
				c.markActive()
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

func (c *Conn) processRead(b []byte) (n int, err error, responseFinished bool) {
	if c.resp == nil {
		if c.req != nil {
			// Stop writing to request body
			c.reqBodyWriter.Close()
		} else {
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

		// Check response status
		responseOK := c.resp.StatusCode >= 200 && c.resp.StatusCode < 300
		if !responseOK {
			respText := bytes.NewBuffer(nil)
			c.resp.Write(respText)
			err = fmt.Errorf("Error writing, response status: %d\n\n", c.resp.StatusCode, string(respText.Bytes()))
			return
		}
	}

	// Read from response body
	n, err = c.resp.Body.Read(b)
	if err == nil {
		// Do an extra read to see if we've reached EOF
		_, err = c.resp.Body.Read([]byte{})
	}
	if err == io.EOF {
		responseFinished = true
		serverEOF := c.resp.Header.Get(X_HTTPCONN_EOF) != ""
		if !serverEOF {
			// Server thinks there may be more to read, suppress EOF to client.
			err = nil
		}
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
func (c *Conn) drainAndCloseChannels() {
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
