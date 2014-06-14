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
	if c.Config.FlushInterval == 0 {
		c.Config.FlushInterval = defaultWriteFlushInterval
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
	c.stopWriteCh = make(chan interface{}, 100)
	c.readRequestsCh = make(chan []byte, 100)
	c.readResponsesCh = make(chan rwResponse, 100)
	c.stopReadCh = make(chan interface{}, 100)
	c.reqOutCh = make(chan *io.PipeReader, 100)
	c.stopReqCh = make(chan interface{}, 100)
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

func (c *Conn) doRequest(proxyConn net.Conn, bufReader *bufio.Reader, host string, method string, body io.ReadCloser) (resp *http.Response, err error) {
	req, err := c.buildRequest(host, method, body)
	if err != nil {
		err = fmt.Errorf("Unable to construct request to proxy: %s", err)
		return
	}

	proxyConn.SetWriteDeadline(time.Now().Add(c.Config.IdleTimeout))
	err = req.Write(proxyConn)
	if err != nil {
		err = fmt.Errorf("Error sending request to proxy: %s", err)
		return
	}

	proxyConn.SetReadDeadline(time.Now().Add(c.Config.IdleTimeout))
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
