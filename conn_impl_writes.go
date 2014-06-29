package enproxy

import (
	"io"
	"time"
)

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
			// We waited more than FlushTimeout for a write, finish our request
			if c.currentBody != nil {
				if !c.finishBody() {
					return
				}
			}
		}
	}
}

// processWrite processes a single write request, encapsulated in the body of a
// POST request to the proxy.  If b is bigger than bodySize (65K), then this
// will result in multiple POST requests.
func (c *Conn) processWrite(b []byte) bool {
	// Consume writes as long as they keep coming in
	bytesWritten := 0

	// Copy from b into outbound body
	for {
		bytesRemaining := bodySize - c.currentBytesRead
		bytesToCopy := len(b)
		if bytesToCopy == 0 {
			break
		} else {
			c.markActive()
			if c.currentBody == nil {
				c.initBody()
			}
			dst := c.currentBody[c.currentBytesRead:]
			if bytesToCopy <= bytesRemaining {
				// Copy the entire buffer to the destination
				copy(dst, b)
				c.currentBytesRead = c.currentBytesRead + bytesToCopy
				bytesWritten = bytesWritten + bytesToCopy
				break
			} else {
				// Copy as much as we can from the buffer to the destination
				copy(dst, b[:bytesRemaining])
				// Set buffer to remaining bytes
				b = b[bytesRemaining:]
				c.currentBytesRead = c.currentBytesRead + bytesRemaining
				bytesWritten = bytesWritten + bytesRemaining
				// Write the body
				if !c.finishBody() {
					return false
				}
			}
		}
	}

	if bodySize == c.currentBytesRead {
		// We've filled the body, write it
		if !c.finishBody() {
			return false
		}
	}

	// Let the caller know how it went
	c.writeResponsesCh <- rwResponse{bytesWritten, nil}
	return true
}

func (c *Conn) initBody() {
	c.currentBody = make([]byte, bodySize)
	c.currentBytesRead = 0
}

func (c *Conn) finishBody() bool {
	body := c.currentBody
	if c.currentBytesRead < len(c.currentBody) {
		body = c.currentBody[:c.currentBytesRead]
	}
	success := c.submitRequest(body)
	if success {
		err := <-c.requestFinishedCh
		if err != nil {
			return false
		}
	}
	c.currentBody = nil
	c.currentBytesRead = 0
	if !success {
		c.writeResponsesCh <- rwResponse{0, io.EOF}
		return false
	}
	return true
}

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

func (c *Conn) cleanupAfterWrites() {
	for {
		select {
		case <-c.writeRequestsCh:
			c.writeResponsesCh <- rwResponse{0, io.EOF}
		case <-c.stopWriteCh:
			// do nothing
		default:
			c.writeMutex.Lock()
			c.doneWriting = true
			c.writeMutex.Unlock()
			close(c.writeRequestsCh)
			return
		}
	}
}
