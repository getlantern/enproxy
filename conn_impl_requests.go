package enproxy

import (
	"io"
	"log"
	"net/http"
	"time"
)

// submitRequest submits a request to the processRequests goroutine, returning
// true if the request was accepted or false if requests are no longer being
// accepted
func (c *Conn) submitRequest(body *io.PipeReader) bool {
	c.requestMutex.RLock()
	defer c.requestMutex.RUnlock()
	if c.doneRequesting {
		return false
	} else {
		c.requestOutCh <- body
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
		case reqBody := <-c.requestOutCh:
			resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "POST", reqBody)
			if err != nil {
				return
			}

			if first {
				// On our first request, find out what host we're actually
				// talking to and remember that for future requests.
				proxyHost = resp.Header.Get(X_ENPROXY_PROXY_HOST)
				// Also post it to proxyHostCh so that the processReads()
				// routine knows which proxyHost to use
				c.proxyHostCh <- proxyHost
				first = false
			}
		case <-c.stopRequestCh:
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
		case <-c.requestOutCh:
			// do nothing
		case <-c.stopRequestCh:
			// do nothing
		default:
			close(c.requestOutCh)
			if resp != nil {
				resp.Body.Close()
			}
			return
		}
	}
}
