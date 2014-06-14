package enproxy

import (
	"io"
	"time"
)

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

func (c *Conn) processWrites() {
	defer c.cleanupAfterWrites()

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.writeRequestsCh:
			if !c.processWrite(b) {
				return
			}
		case <-c.stopWriteCh:
			return
		case <-time.After(c.Config.FlushTimeout):
			if c.isIdle() {
				// Connection is idle, stop writing
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

func (c *Conn) processWrite(b []byte) bool {
	// Consume writes as long as they keep coming in
	if c.reqBodyWriter == nil {
		// Lazily initialize our next request to the proxy
		// Construct a pipe for piping data to proxy
		reqBody, reqBodyWriter := io.Pipe()
		c.reqBodyWriter = reqBodyWriter
		if !c.submitRequest(reqBody) {
			c.writeResponsesCh <- rwResponse{0, io.EOF}
			return false
		}
	}

	// Write out data to the request body
	n, err := c.reqBodyWriter.Write(b)
	if n > 0 {
		c.markActive()
	}

	// Let the caller know how it went
	c.writeResponsesCh <- rwResponse{n, err}
	if err != nil {
		return false
	}

	return true
}

func (c *Conn) cleanupAfterWrites() {
	c.writeMutex.Lock()
	c.doneWriting = true
	c.writeMutex.Unlock()
	for {
		select {
		case <-c.writeRequestsCh:
			c.writeResponsesCh <- rwResponse{0, io.EOF}
		case <-c.stopWriteCh:
			// do nothing
		default:
			close(c.writeRequestsCh)
			return
		}
	}
}
