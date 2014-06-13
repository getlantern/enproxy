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
	c.writeRequestsCh = make(chan []byte)
	c.writeResponsesCh = make(chan rwResponse)
	c.readRequestsCh = make(chan []byte)
	c.readResponsesCh = make(chan rwResponse)
	c.reqOutCh = make(chan *io.PipeReader)
}

func (c *Conn) processWrites() {
	defer func() {
		// c.writeMutex.Lock()
		// defer c.writeMutex.Unlock()
		for {
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
	var proxyConn net.Conn
	var bufReader *bufio.Reader
	var resp *http.Response
	var err error

	defer func() {
		// c.readMutex.Lock()
		// defer c.readMutex.Unlock()
		for {
			select {
			case <-c.readRequestsCh:
				c.readResponsesCh <- rwResponse{0, io.EOF}
			default:
				c.doneReading = true
				close(c.readRequestsCh)
				if proxyConn != nil {
					proxyConn.Close()
				}
				if resp != nil {
					resp.Body.Close()
				}
				return
			}
		}
	}()

	// Dial proxy
	proxyConn, bufReader, err = c.dialProxy()
	if err != nil {
		log.Printf("Unable to dial proxy to GET data: %s", err)
		return
	}

	// Wait for proxy host from first request
	proxyHost := <-c.proxyHostCh

	log.Println("Making read request")
	resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "GET", nil)
	if err != nil {
		log.Printf("Unable to do GET request: %s", err)
		return
	}

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.readRequestsCh:
			if resp == nil {
				log.Println("Making read request")
				proxyConn, bufReader, err = c.redialProxyIfNecessary(proxyConn, bufReader)
				if err != nil {
					log.Printf("Unable to redial proxy for GETing request: %s", err)
					return
				}

				resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "GET", nil)
				if err != nil {
					log.Printf("Unable to do GET request: %s", err)
					return
				}
			}

			if resp.Header.Get(X_HTTPCONN_EOF) == "true" {
				c.readResponsesCh <- rwResponse{0, io.EOF}
				return
			}

			// Read
			n, err := resp.Body.Read(b)
			if n > 0 {
				c.markActive()
			}

			errToClient := err
			if err == io.EOF {
				// Don't propagate EOF to client
				errToClient = nil
			}
			c.readResponsesCh <- rwResponse{n, errToClient}
			if err != nil {
				if err == io.EOF {
					resp.Body.Close()
					resp = nil
					continue
				} else {
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
	var proxyConn net.Conn
	var bufReader *bufio.Reader
	var resp *http.Response
	var err error

	defer func() {
		// c.requestMutex.Lock()
		// defer c.requestMutex.Unlock()
		for {
			select {
			case <-c.reqOutCh:
			default:
				c.doneRequesting = true
				close(c.reqOutCh)
				if proxyConn != nil {
					proxyConn.Close()
				}
				if resp != nil {
					resp.Body.Close()
				}
				return
			}
		}
	}()

	// Dial proxy
	proxyConn, bufReader, err = c.dialProxy()
	if err != nil {
		log.Printf("Unable to dial proxy for POSTing request: %s", err)
		return
	}

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

			proxyConn, bufReader, err = c.redialProxyIfNecessary(proxyConn, bufReader)
			if err != nil {
				log.Printf("Unable to redial proxy for POSTing request: %s", err)
				return
			}

			resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "POST", reqBody)
			if err != nil {
				log.Printf("Unable to do POST request: %s", err)
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

func (c *Conn) dialProxy() (proxyConn net.Conn, bufReader *bufio.Reader, err error) {
	proxyConn, err = c.Config.DialProxy(c.Addr)
	if err != nil {
		err = fmt.Errorf("Unable to dial proxy: %s", err)
		return
	}
	bufReader = bufio.NewReader(proxyConn)
	return
}

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

func (c *Conn) doRequest(proxyConn net.Conn, bufReader *bufio.Reader, host string, method string, body io.ReadCloser) (resp *http.Response, err error) {
	req, err := c.buildRequest(host, method, body)
	if err != nil {
		err = fmt.Errorf("Unable to construct request to proxy: %s", err)
		return
	}

	err = req.Write(proxyConn)
	if err != nil {
		err = fmt.Errorf("Error sending request to proxy: %s", err)
		return
	}

	resp, err = http.ReadResponse(bufReader, req)
	if err != nil {
		err = fmt.Errorf("Error reading response from proxy: %s", err)
		return
	}

	// Check response status
	responseOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !responseOK {
		err = fmt.Errorf("Bad response status for read: %s", resp.Status)
		resp.Body.Close()
		resp = nil
	}

	return
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
	// c.lastActivityMutex.Lock()
	// defer c.lastActivityMutex.Unlock()
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
	if c.doneWriting {
		c.writeMutex.RUnlock()
		return false
	} else {
		c.writeMutex.RUnlock()
		c.writeRequestsCh <- b
		return true
	}
}

func (c *Conn) submitRead(b []byte) bool {
	c.readMutex.RLock()
	if c.doneReading {
		c.readMutex.RUnlock()
		return false
	} else {
		c.readMutex.RUnlock()
		c.readRequestsCh <- b
		return true
	}
}

func (c *Conn) submitRequest(body *io.PipeReader) bool {
	c.requestMutex.RLock()
	if c.doneRequesting {
		c.requestMutex.RUnlock()
		return false
	} else {
		c.requestMutex.RUnlock()
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
