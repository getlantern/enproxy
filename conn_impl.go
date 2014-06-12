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
func (c *Conn) Connect() (err error) {
	// Generate a unique id for this connection.  This is used by the Proxy to
	// associate requests from this connection to the corresponding outbound
	// connection on the proxy side.
	c.id = uuid.NewRandom().String()

	c.initDefaults()
	c.makeChannels()
	c.markActive()

	go c.processWrites()
	go c.processReads()
	go c.postRequests()

	return
}

func (c *Conn) initDefaults() {
	if c.Config.PollInterval == 0 {
		c.Config.PollInterval = defaultPollInterval
	}
	if c.Config.DefaultTimeoutProfile == nil {
		c.Config.DefaultTimeoutProfile = defaultTimoutProfile
	}
	if c.Config.TimeoutProfilesByPort == nil {
		c.Config.TimeoutProfilesByPort = make(map[string]*TimeoutProfile)
	}
	for port, defaultProfile := range defaultWriteTimeoutProfilesByPort {
		_, exists := c.Config.TimeoutProfilesByPort[port]
		if !exists {
			// Merge default into map
			c.Config.TimeoutProfilesByPort[port] = defaultProfile
		}
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
		default:
			close(c.writeRequestsCh)
			close(c.readRequestsCh)
			close(c.nextRequestCh)
			return
		}
	}
}

// postRequests handles writing outbound requests to the proxy.  Note - this is
// not pipelined, because we cannot be sure that intervening proxies will
// deliver requests to the enproxy server in order. In-order delivery is
// required because we are encapsulating a stream of data inside the bodies of
// successive requests.
func (c *Conn) postRequests() {
	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, err := c.Config.DialProxy(c.Addr)
	if err != nil {
		log.Printf("Unable to dial proxy: %s", err)
		c.Close()
		return
	}
	defer proxyConn.Close()
	bufReader := bufio.NewReader(proxyConn)

	for {
		req, ok := <-c.nextRequestCh
		log.Println("postRequests() - Got outbound request")
		if !ok {
			// done processing requests
			return
		}

		// Write request
		err := req.Write(proxyConn)
		if err != nil {
			log.Printf("Unexpected error writing write request: %s", err)
			return
		}

		// Read corresponding response
		resp, err := http.ReadResponse(bufReader, nil)
		if err != nil {
			log.Printf("Unexpected error reading write response: %s", err)
			return
		}

		// Check response status
		responseOK := resp.StatusCode >= 200 && resp.StatusCode < 300
		if !responseOK {
			respText := bytes.NewBuffer(nil)
			resp.Write(respText)
			log.Printf("Bad response status for write: %d\n%s\n", resp.StatusCode, string(respText.Bytes()))
			return
		}

		log.Println("postRequests() - Wrote outbound request ok")

		// proxyHost := resp.Header.Get(X_HTTPCONN_PROXY_HOST)
		// if proxyHost != "" {
		// 	c.proxyHost = proxyHost
		// }
	}
}

func (c *Conn) processWrites() {
	port := strings.Split(c.Addr, ":")[1]
	tp := c.Config.TimeoutProfilesByPort[port]
	if tp == nil {
		tp = c.Config.DefaultTimeoutProfile
	}

	for {
		if c.isClosed() {
			return
		}

		timeout := 35 * time.Millisecond
		select {
		case b := <-c.writeRequestsCh:
			log.Println("processWrites() - Got write request")
			// Consume writes as long as they keep coming in
			if !c.processWrite(b) {
				log.Println("processWrites() - Problem processing write")
				// Problem writing, stop processing
				c.Close()
				return
			} else {
				log.Println("processWrites() - Processed write")
				c.markActive()
			}
		case <-time.After(timeout):
			if c.isIdle() {
				// No activity for more than IdleTimeout, stop processing
				c.Close()
				return
			}
			// We waited more than PollInterval for a write, close our request
			// body writer so that it can get flushed to the server
			if c.reqBodyWriter != nil {
				c.reqBodyWriter.Close()
				c.reqBodyWriter = nil
			}
		}
	}
}

// processWrite processes a single write, returning true if the write was
// successful, false otherwise.
func (c *Conn) processWrite(b []byte) (ok bool) {
	if c.reqBodyWriter == nil {
		log.Println("processWrite() - initializing new write request")
		// Lazily initialize our next request to the proxy
		err := c.initRequestToProxy()
		if err != nil {
			c.writeResponsesCh <- rwResponse{0, err}
			return false
		}
		log.Println("processWrite() - initialized new write request")
	}

	// Write out data to the request body
	n, err := c.reqBodyWriter.Write(b)
	if err != nil {
		c.writeResponsesCh <- rwResponse{n, fmt.Errorf("Unable to write to proxy pipe: %s", err)}
		return false
	}
	log.Printf("processWrite() - wrote %d bytes", n)

	// Let the caller know how much we wrote
	c.writeResponsesCh <- rwResponse{n, nil}
	log.Println("processWrite() - returned write response")
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
func (c *Conn) processReads() {
	log.Println("processReads() - Processing reads")
	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, err := c.Config.DialProxy(c.Addr)
	if err != nil {
		log.Printf("Unable to dial proxy: %s", err)
		return
	}
	defer proxyConn.Close()
	bufReader := bufio.NewReader(proxyConn)

	log.Println("processReads() - Connected to proxy")

	req, err := c.Config.NewRequest(c.proxyHost, "GET", nil)
	if err != nil {
		log.Printf("Unable to construct request to proxy: %s", err)
		return
	}
	// Send our connection id
	req.Header.Set(X_HTTPCONN_ID, c.id)
	// Send the address that we're trying to reach
	req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)

	err = req.Write(proxyConn)
	if err != nil {
		log.Printf("Error requesting read response data: %s", err)
		return
	}
	log.Println("processReads() - Wrote read request")

	resp, err := http.ReadResponse(bufReader, req)
	if err != nil {
		log.Printf("Error reading read response: %s", err)
		return
	}
	defer resp.Body.Close()
	log.Println("processReads() - Read read resonse")

	// Check response status
	responseOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !responseOK {
		respText := bytes.NewBuffer(nil)
		resp.Write(respText)
		log.Printf("Bad response status for read: %d\n%s\n", resp.StatusCode, string(respText.Bytes()))
		return
	}
	log.Println("processReads() - Read response ok")

	for {
		b := <-c.readRequestsCh
		log.Println("processReads() - Got read request")
		n, err := resp.Body.Read(b)
		if n > 0 {
			log.Printf("processReads() - read %d bytes, error: %s", n, err)
			c.markActive()
		}
		log.Println("processReads() - posting response")
		c.readResponsesCh <- rwResponse{n, err}
		log.Println("processReads() - posted response")
		if err != nil {
			if err != io.EOF {
				log.Printf("Unexpected error reading from proxyConn: %s", err)
			}
			return
		}
	}
	return
}

// post posts a request with the given body to the proxy.  If writeImmediate is
// true, the request is written before post returns.  Otherwise, the request is
// written on a goroutine and post returns immediately.
func (c *Conn) post(requestBody io.ReadCloser) (err error) {
	req, err := c.Config.NewRequest(c.proxyHost, "POST", requestBody)
	if err != nil {
		return fmt.Errorf("Unable to construct request to proxy: %s", err)
	}
	// Send our connection id
	req.Header.Set(X_HTTPCONN_ID, c.id)
	// Send the address that we're trying to reach
	req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)

	c.nextRequestCh <- req
	return nil
}

func (c *Conn) markActive() {
	c.lastActivityMutex.Lock()
	defer c.lastActivityMutex.Unlock()
	c.lastActivityTime = time.Now()
}

func (c *Conn) isIdle() bool {
	c.lastActivityMutex.RLock()
	defer c.lastActivityMutex.RUnlock()
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
