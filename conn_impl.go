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
	if c.Config.FlushTimeout == 0 {
		c.Config.FlushTimeout = defaultWriteFlushTimeout
	}
	if c.Config.IdleTimeout == 0 {
		c.Config.IdleTimeout = defaultIdleTimeoutClient
	}
}

func (c *Conn) makeChannels() {
	c.initialResponseCh = make(chan hostWithResponse)
	c.writeRequestsCh = make(chan []byte)
	c.writeResponsesCh = make(chan rwResponse)
	c.stopWriteCh = make(chan interface{}, closeChannelDepth)
	c.readRequestsCh = make(chan []byte)
	c.readResponsesCh = make(chan rwResponse)
	c.stopReadCh = make(chan interface{}, closeChannelDepth)
	c.requestOutCh = make(chan []byte)
	c.requestFinishedCh = make(chan error)
	c.stopRequestCh = make(chan interface{}, closeChannelDepth)
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
// closed somehow.  This will happen especially when using an intermediary proxy
// like a CDN, which will sometimes aggressively close idle connections.
func (c *Conn) redialProxyIfNecessary(origProxyConn net.Conn, origBufReader *bufio.Reader) (proxyConn net.Conn, bufReader *bufio.Reader, err error) {
	// Default to keeping the same connection
	proxyConn = origProxyConn
	bufReader = origBufReader

	// Make sure connection is still open and redial if necessary
	origProxyConn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	_, err = origBufReader.Peek(1)
	origProxyConn.SetReadDeadline(time.Time{})
	if err == io.EOF {
		// Close original connection
		origProxyConn.Close()
		// Dial again
		proxyConn, bufReader, err = c.dialProxy()
		if err != nil {
			log.Println("Unable to redial proxy: %s", err)
			return
		}
	} else {
		err = nil
	}
	return
}

func (c *Conn) doRequest(proxyConn net.Conn, bufReader *bufio.Reader, host string, op string, bodyBytes []byte) (resp *http.Response, err error) {
	var body io.Reader
	if bodyBytes != nil {
		body = &closer{bytes.NewReader(bodyBytes)}
	}
	req, err := c.Config.NewRequest(host, "POST", body)
	if err != nil {
		err = fmt.Errorf("Unable to construct request to proxy: %s", err)
		return
	}
	req.Header.Set(X_ENPROXY_OP, op)
	// Always send our connection id
	req.Header.Set(X_ENPROXY_ID, c.id)
	// Always send the address that we're trying to reach
	req.Header.Set(X_ENPROXY_DEST_ADDR, c.Addr)
	req.Header.Set("Content-type", "application/octet-stream")
	if bodyBytes != nil {
		// Always force identity encoding to appeas CDNs like Fastly that can't
		// handle chunked encoding on requests
		req.TransferEncoding = []string{"identity"}
		req.ContentLength = int64(len(bodyBytes))
	} else {
		req.ContentLength = 0
	}

	// Important - we set WriteDeadline and ReadDeadline separately instead of
	// calling SetDeadline because we will later change the read and write
	// deadlines independently.

	// Don't spend more than IdleTimeout trying to write to request
	proxyConn.SetWriteDeadline(time.Now().Add(c.Config.IdleTimeout))
	err = req.Write(proxyConn)
	if err != nil {
		err = fmt.Errorf("Error sending request to proxy: %s", err)
		return
	}

	// Don't spend more than IdleTimeout trying to read from response
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
		Body:       &closer{strings.NewReader(msg)},
	}
	resp.Write(w)
}

type closer struct {
	io.Reader
}

func (r *closer) Close() error {
	return nil
}
