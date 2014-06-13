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
	go c.processRequests()

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
	c.proxyHostCh = make(chan string)
	c.writeRequestsCh = make(chan []byte, 100)
	c.writeResponsesCh = make(chan rwResponse, 100)
	c.readRequestsCh = make(chan []byte, 100)
	c.readResponsesCh = make(chan rwResponse, 100)
	c.reqOutCh = make(chan *io.PipeReader, 100)
}

func (c *Conn) processWrites() {
	defer func() {
		for {
			c.writeMutex.Lock()
			defer c.writeMutex.Unlock()
			select {
			case <-c.writeRequestsCh:
				c.writeResponsesCh <- rwResponse{0, io.EOF}
			default:
				c.doneWriting = true
				close(c.writeRequestsCh)
				return
			}
		}
	}()

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.writeRequestsCh:
			// Consume writes as long as they keep coming in
			if !c.processWrite(b) {
				return
			} else {
				c.markActive()
			}
		case <-time.After(c.Config.IdleInterval):
			if c.isIdle() {
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

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request.
func (c *Conn) processReads() {
	defer func() {
		for {
			c.readMutex.Lock()
			defer c.readMutex.Unlock()
			select {
			case <-c.readRequestsCh:
				c.readResponsesCh <- rwResponse{0, io.EOF}
			default:
				c.doneReading = true
				close(c.readRequestsCh)
				return
			}
		}
	}()

	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, err := c.Config.DialProxy(c.Addr)
	if err != nil {
		log.Printf("Unable to dial proxy: %s", err)
		return
	}
	defer proxyConn.Close()
	bufReader := bufio.NewReader(proxyConn)

	// Wait for proxy host from first request
	proxyHost := <-c.proxyHostCh

	req, err := c.buildRequest(proxyHost, "GET", nil)
	if err != nil {
		log.Printf("Unable to construct request to proxy: %s", err)
		return
	}

	err = req.Write(proxyConn)
	if err != nil {
		log.Printf("Error requesting read response data: %s", err)
		return
	}

	resp, err := http.ReadResponse(bufReader, req)
	if err != nil {
		log.Printf("Error reading read response: %s", err)
		return
	}
	defer resp.Body.Close()

	// Check response status
	responseOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !responseOK {
		respText := bytes.NewBuffer(nil)
		resp.Write(respText)
		log.Printf("Bad response status for read: %d\n%s\n", resp.StatusCode, string(respText.Bytes()))
		return
	}

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.readRequestsCh:
			n, err := resp.Body.Read(b)
			if n > 0 {
				c.markActive()
			}
			c.readResponsesCh <- rwResponse{n, err}
			if err != nil {
				if err != io.EOF {
					log.Printf("Unexpected error reading from proxyConn: %s", err)
				}
				return
			}
		case <-time.After(c.Config.IdleTimeout):
			// Haven't read within our idle timeout, continue loop
		}
	}
	return
}

// processRequests handles writing outbound requests to the proxy.  Note - this
// is not pipelined, because we cannot be sure that intervening proxies will
// deliver requests to the enproxy server in order. In-order delivery is
// required because we are encapsulating a stream of data inside the bodies of
// successive requests.
func (c *Conn) processRequests() {
	defer func() {
		for {
			c.requestMutex.Lock()
			defer c.requestMutex.Unlock()
			select {
			case <-c.reqOutCh:
			default:
				c.doneRequesting = true
				close(c.reqOutCh)
				return
			}
		}
	}()

	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, err := c.Config.DialProxy(c.Addr)
	if err != nil {
		log.Printf("Unable to dial proxy: %s", err)
		return
	}
	defer proxyConn.Close()
	bufReader := bufio.NewReader(proxyConn)

	var proxyHost string
	first := true

	for {
		if c.isClosed() {
			return
		}

		select {
		case reqBody, ok := <-c.reqOutCh:
			if !ok {
				// done processing requests
				return
			}

			// Construct a new HTTP POST to encapsulate our data
			req, err := c.buildRequest(proxyHost, "POST", reqBody)
			if err != nil {
				log.Printf("Unable to construct POST request to proxy: %s", err)
			}

			// Write request
			err = req.Write(proxyConn)
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

			if first {
				// Lazily initialize proxyHost
				proxyHost = resp.Header.Get(X_HTTPCONN_PROXY_HOST)
				c.proxyHostCh <- proxyHost
				first = false
			}
		case <-time.After(c.Config.IdleTimeout):
			// Hit idle timeout without getting a new request, continue loop
		}
	}
}

// processWrite processes a single write, returning true if the write was
// successful, false otherwise.
func (c *Conn) processWrite(b []byte) (ok bool) {
	if c.reqBodyWriter == nil {
		// Lazily initialize our next request to the proxy
		// Construct a pipe for piping data to proxy
		reqBody, reqBodyWriter := io.Pipe()
		c.reqBodyWriter = reqBodyWriter
		if !c.submitRequest(reqBody) {
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

// buildRequest builds a request using the given parameters and adding the
// enproxy-specific headers.
func (c *Conn) buildRequest(host string, method string, requestBody io.ReadCloser) (req *http.Request, err error) {
	req, err = c.Config.NewRequest(host, method, requestBody)
	if err != nil {
		return nil, fmt.Errorf("Unable to construct request to proxy: %s", err)
	}
	// Send our connection id
	req.Header.Set(X_HTTPCONN_ID, c.id)
	// Send the address that we're trying to reach
	req.Header.Set(X_HTTPCONN_DEST_ADDR, c.Addr)
	return
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

func (c *Conn) submitWrite(b []byte) bool {
	c.writeMutex.RLock()
	defer c.writeMutex.RUnlock()
	if c.doneWriting {
		return false
	} else {
		c.writeRequestsCh <- b
		return true
	}
}

func (c *Conn) submitRead(b []byte) bool {
	c.readMutex.RLock()
	defer c.readMutex.RUnlock()
	if c.doneReading {
		return false
	} else {
		c.readRequestsCh <- b
		return true
	}
}

func (c *Conn) submitRequest(body *io.PipeReader) bool {
	c.requestMutex.RLock()
	defer c.requestMutex.RUnlock()
	if c.doneRequesting {
		return false
	} else {
		c.reqOutCh <- body
		return true
	}
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
