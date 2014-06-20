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
		c.Config.IdleTimeout = defaultIdleTimeout
	}
}

func (c *Conn) makeChannels() {
	c.initialResponseCh = make(chan hostWithResponse)
	c.writeRequestsCh = make(chan []byte, channelDepth)
	c.writeResponsesCh = make(chan rwResponse, channelDepth)
	c.stopWriteCh = make(chan interface{}, channelDepth)
	c.readRequestsCh = make(chan []byte, channelDepth)
	c.readResponsesCh = make(chan rwResponse, channelDepth)
	c.stopReadCh = make(chan interface{}, channelDepth)
	c.requestOutCh = make(chan *io.PipeReader, channelDepth)
	c.stopRequestCh = make(chan interface{}, channelDepth)
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

func (c *Conn) doRequest(proxyConn net.Conn, bufReader *bufio.Reader, host string, op string, body io.ReadCloser) (resp *http.Response, err error) {
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

	// Don't spend more than IdleTimeout trying to read from request
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
