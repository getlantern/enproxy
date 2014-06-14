package enproxy

import (
	"io"
	"time"
)

// submitWrite submits a write to the processWrites goroutine, returning true if
// the write was accepted or false if writes are no longer being accepted
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

// processWrites processes write requests by writing them to the body of a POST
// request.  Note - processWrites doesn't actually send the POST requests,
// that's handled by the processRequests goroutine.  The reason that we do this
// on a separate goroutine is that the call to Request.Write() blocks until the
// body has finished, and of course the body is written to as a result of
// processing writes, so we need 2 goroutines to allow us to continue to
// accept writes and pipe these to the request body while actually sending that
// request body to the server.
func (c *Conn) processWrites() {
	defer c.cleanupAfterWrites()

	for {
		if c.isClosed() {
			return
		}

		select {
		case b := <-c.writeRequestsCh:
			if !c.processWrite(b) {
				// There was a problem processing a write, stop
				return
			}
		case <-c.stopWriteCh:
			return
		case <-time.After(c.Config.FlushTimeout):
			if c.isIdle() {
				// Connection is idle, stop writing
				return
			}
			// We waited more than FlushTimeout for a write, close our request
			// body writer so that it can get flushed to the server
			if c.reqBodyWriter != nil {
				c.reqBodyWriter.Close()
				c.reqBodyWriter = nil
			}
		}
	}
}

// processWrite processes a single write request, lazily starting a new POST
// to the proxy when necessary.
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
