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
}

func (c *Conn) processWrites() {
	defer func() {
		for {
			select {
			case <-c.writeRequestsCh:
				c.writeResponsesCh <- rwResponse{0, io.EOF}
			default:
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
				// Problem writing, stop processing
				c.Close()
				return
			} else {
				c.markActive()
			}
		case <-time.After(c.Config.IdleInterval):
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

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request.
func (c *Conn) processReads() {
	defer func() {
		for {
			select {
			case <-c.readRequestsCh:
				c.readResponsesCh <- rwResponse{0, io.EOF}
			default:
				close(c.readRequestsCh)
				return
			}
		}
	}()

	// Dial proxy (we do this inside here so that it's on a goroutine and
	// doesn't block the call to Conn.Start().
	proxyConn, err := c.Config.DialProxy(c.Addr)
	if err != nil {
		return
	}
	defer proxyConn.Close()
	bufReader := bufio.NewReader(proxyConn)

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

// postRequests handles writing outbound requests to the proxy.  Note - this is
// not pipelined, because we cannot be sure that intervening proxies will
// deliver requests to the enproxy server in order. In-order delivery is
// required because we are encapsulating a stream of data inside the bodies of
// successive requests.
func (c *Conn) postRequests() {
	defer func() {
		for {
			select {
			case <-c.nextRequestCh:
				// ignore
			default:
				close(c.nextRequestCh)
				return
			}
		}
	}()

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
		if c.isClosed() {
			return
		}

		select {
		case req, ok := <-c.nextRequestCh:
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

			// proxyHost := resp.Header.Get(X_HTTPCONN_PROXY_HOST)
			// if proxyHost != "" {
			// 	c.proxyHost = proxyHost
			// }
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
	return c.post(c.reqBody)
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
