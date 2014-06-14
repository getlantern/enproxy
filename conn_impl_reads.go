package enproxy

import (
	"io"
	"log"
	"net/http"
	"time"
)

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

// processReads processes read requests until EOF is reached on the response to
// our encapsulated HTTP request, or we've hit our idle interval and still
// haven't received a read request.
func (c *Conn) processReads() {
	var resp *http.Response

	defer c.cleanupAfterReads(resp)

	// Dial proxy
	proxyConn, bufReader, err := c.dialProxy()
	if err != nil {
		log.Printf("Unable to dial proxy to GET data: %s", err)
		return
	}
	defer proxyConn.Close()

	// Wait for proxy host from first request
	proxyHost := <-c.proxyHostCh

	resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "GET", nil)
	if err != nil {
		return
	}

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.readRequestsCh:
			if resp == nil {
				resp, err = c.doRequest(proxyConn, bufReader, proxyHost, "GET", nil)
				if err != nil {
					c.readResponsesCh <- rwResponse{0, io.EOF}
					return
				}
			}

			if resp.Header.Get(X_HTTPCONN_EOF) == "true" {
				c.readResponsesCh <- rwResponse{0, io.EOF}
				return
			}

			// Process read
			proxyConn.SetReadDeadline(time.Now().Add(c.Config.IdleTimeout))
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
					return
				}
			}
		case <-c.stopReadCh:
			return
		case <-time.After(c.Config.IdleTimeout):
			if c.isIdle() {
				return
			}
		}
	}
	return
}

func (c *Conn) cleanupAfterReads(resp *http.Response) {
	c.readMutex.Lock()
	c.doneReading = true
	c.readMutex.Unlock()
	for {
		select {
		case <-c.readRequestsCh:
			c.readResponsesCh <- rwResponse{0, io.EOF}
		case <-c.stopReadCh:
			// do nothing
		default:
			close(c.readRequestsCh)
			if resp != nil {
				resp.Body.Close()
			}
			return
		}
	}
}
