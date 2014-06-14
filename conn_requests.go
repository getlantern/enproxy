package enproxy

import (
	"io"
	"log"
	"net/http"
	"time"
)

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

// processRequests handles writing outbound requests to the proxy.  Note - this
// is not pipelined, because we cannot be sure that intervening proxies will
// deliver requests to the enproxy server in order. In-order delivery is
// required because we are encapsulating a stream of data inside the bodies of
// successive requests.
func (c *Conn) processRequests() {
	var resp *http.Response

	defer c.cleanupAfterRequests(resp)

	// Dial proxy
	proxyConn, bufReader, err := c.dialProxy()
	if err != nil {
		log.Printf("Unable to dial proxy for POSTing request: %s", err)
		return
	}
	defer proxyConn.Close()

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

			resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "POST", reqBody)
			if err != nil {
				return
			}

			if first {
				// Lazily initialize proxyHost
				proxyHost = resp.Header.Get(X_HTTPCONN_PROXY_HOST)
				c.proxyHostCh <- proxyHost
				first = false
			}
		case <-c.stopReqCh:
			return
		case <-time.After(c.Config.IdleTimeout):
			if c.isIdle() {
				return
			}
		}
	}
}

func (c *Conn) cleanupAfterRequests(resp *http.Response) {
	c.requestMutex.Lock()
	c.doneRequesting = true
	c.requestMutex.Unlock()
	for {
		select {
		case <-c.reqOutCh:
			// do nothing
		case <-c.stopReqCh:
			// do nothing
		default:
			close(c.reqOutCh)
			if resp != nil {
				resp.Body.Close()
			}
			return
		}
	}
}
