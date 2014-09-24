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
			err := c.rs.finishBody()
			if err != nil {
				c.writeResponsesCh <- rwResponse{0, err}
				return
			}
		}
	}
}

// processWrite processes a single write request, encapsulated in the body of a
// POST request to the proxy. It uses the configured requestStrategy to process
// the request. It returns true if the write was successful.
func (c *Conn) processWrite(b []byte) bool {
	n, err := c.rs.write(b)
	c.writeResponsesCh <- rwResponse{n, err}
	return err == nil
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
	panicked := recover()

	for {
		select {
		case <-c.writeRequestsCh:
			if panicked != nil {
				c.writeResponsesCh <- rwResponse{0, io.ErrUnexpectedEOF}
			} else {
				c.writeResponsesCh <- rwResponse{0, io.EOF}
			}
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
