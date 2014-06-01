// enproxy provides a chained HTTP proxy (client+server) that encapsulates
// traffic inside the bodies of HTTP requests and responses.  This is useful for
// tunneling traffic across reverse proxies and CDNs.
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

const (
	CONNECT = "CONNECT"
)

type NewRequestFunc func(method string, body io.ReadCloser) (*http.Request, error)

// Client is an HTTP proxy that tunnels its data via an httpconn.Server using
// requests and responses.  It assumes that streaming requests are not supported
// by the underlying servers/proxies, and so uses a polling technique similar to
// the one used by meek, but different in that data is not encoded as JSON.
// https://trac.torproject.org/projects/tor/wiki/doc/AChildsGardenOfPluggableTransports#Undertheencryption.
type Client struct {
	// NewRequest: function to create a new request to the server
	NewRequest NewRequestFunc

	// HttpClient: client used to send encapsulated HTTP requests
	HttpClient *http.Client

	// IdleInterval: how long to wait for a read to complete before switching to
	// writing
	IdleInterval time.Duration

	// PollInterval: how long to wait before sending an empty request to server
	// to poll for data.
	PollInterval time.Duration

	// IdleTimeout: how long to wait before closing an idle inbound connection
	IdleTimeout time.Duration
}

// ListenAndServe starts up a new net.Listener at the given addr.  This
// listener is an HTTP proxy that is able to handle CONNECT requests.
func (c *Client) ListenAndServe(addr string) error {
	c.defaultTimeouts()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %s", err)
			continue
		}
		go c.handleIncomingConn(conn)
	}
}

// defaultTimeouts sets default timeouts for any that weren't specified already.
func (c *Client) defaultTimeouts() {
	if c.IdleInterval == 0 {
		c.IdleInterval = defaultIdleInterval
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
}

// handleIncomingConn handles an incoming connection, reading requests from it
// and processing those until the incoming connection is closed or an error is
// encountered.
func (c *Client) handleIncomingConn(conn net.Conn) {
	// Add idle timing to conn
	connIn := newIdleTimingConn(conn, c.IdleTimeout, nil)
	// Set up a buffered reader
	reader := bufio.NewReader(connIn)

	defer connIn.Close()

	// Set up a globally unique connection id
	connId := uuid.NewRandom().String()
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			log.Printf("Done reading requests: %s", err)
			return
		}
		log.Printf("Read request: %s", req)
		ctx := &requestContext{
			c:      c,
			connIn: connIn,
			reader: reader,
			connId: connId,
			req:    req,
		}
		if !ctx.handle() {
			return
		}
	}
}

// requestContext is a context for handling an inbound request
type requestContext struct {
	c           *Client
	connIn      *idleTimingConn
	reader      *bufio.Reader
	connId      string
	req         *http.Request
	addr        string
	isTunneling bool
}

// handle handles an inbound request, returning true if it's okay to continue
// reading additional inbound requests, or false if processing should stop.
func (ctx *requestContext) handle() bool {
	ctx.isTunneling = ctx.req.Method == CONNECT
	ctx.addr = hostIncludingPort(ctx.req, ctx.isTunneling)

	if ctx.isTunneling {
		OK(ctx.connIn)
		return ctx.processAdditionalData()
	} else {
		respOut, err := ctx.copyRequestToServer()
		if err != nil {
			BadGateway(ctx.connIn, err.Error())
			return false
		}
		go func() {
			needsMoreData, _ := ctx.copyResponseFromServer(respOut)
			if needsMoreData {
				ctx.processAdditionalData()
			}
		}()
		return true
	}
}

// copyRequestToServer copies the current inbound request to the server and
// returns the resulting response
func (ctx *requestContext) copyRequestToServer() (*http.Response, error) {
	requestBody, requestBodyWriter := io.Pipe()
	go func() {
		ctx.req.Write(requestBodyWriter)
		requestBodyWriter.Close()
	}()

	reqOut, err := ctx.buildRequestOut(requestBody)
	if err != nil {
		return nil, fmt.Errorf("Unable to construct outbound request: %s", err)
	}

	respOut, err := ctx.c.HttpClient.Do(reqOut)
	if err != nil {
		return nil, err
	}
	if respOut.StatusCode != 200 {
		b := bytes.NewBuffer(nil)
		io.Copy(b, respOut.Body)
		return nil, fmt.Errorf("Unexpected response status from server: %d: %s", respOut.StatusCode, string(b.Bytes()))
	}

	return respOut, nil
}

// processAdditionalData processes additional data from the inbound conn and
// polls for additional response data from the outbound conn.
// Returns true if it's okay to continue reading additional inbound requests,
// or false of processing on the current inbound connection should stop.
func (ctx *requestContext) processAdditionalData() bool {
	for {
		var ireader *impatientIn
		var reader io.ReadCloser
		if ctx.isTunneling {
			// Continue to read from client
			ireader = &impatientIn{
				orig:         ctx.connIn,
				reader:       ctx.reader,
				idleInterval: ctx.c.IdleInterval,
				pollInterval: ctx.c.PollInterval,
				startTime:    time.Now(),
			}
			reader = ireader
		} else {
			// Don't read from client, just wait for poll interval
			time.Sleep(ctx.c.PollInterval)
		}
		reqOut, err := ctx.buildRequestOut(reader)
		if err != nil {
			return false
		}
		respOut, err := ctx.c.HttpClient.Do(reqOut)
		if err != nil || respOut.StatusCode != 200 {
			return false
		}
		if ctx.isTunneling {
			if ireader.hitEOF {
				// connIn has closed, stop processing
				return false
			}
		}
		moreToRead, err := ctx.copyResponseFromServer(respOut)
		if !moreToRead {
			// The server has told us that there's nothing more to read
			return err == nil
		}
	}
}

// buildRequestOut builds an outbound request to the server
func (ctx *requestContext) buildRequestOut(requestBody io.ReadCloser) (*http.Request, error) {
	reqOut, err := ctx.c.NewRequest("POST", requestBody)
	if err == nil {
		// Set our ID
		reqOut.Header.Set(X_HTTPCONN_ID, ctx.connId)
		// Set the address that we're trying to reach
		reqOut.Header.Set(X_HTTPCONN_DEST_ADDR, ctx.addr)
	}
	return reqOut, err
}

func hostIncludingPort(req *http.Request, https bool) string {
	parts := strings.Split(req.Host, ":")
	if len(parts) == 1 {
		port := 80
		if https {
			port = 443
		}
		return fmt.Sprintf("%s:%d", req.Host, port)
	} else {
		return req.Host
	}
}

// copyResponse copies the response from server to the inbound connection.
// If there is no more data to read (or there was an error) it returns false,
// otherwise it returns true.
func (ctx *requestContext) copyResponseFromServer(respOut *http.Response) (moreToRead bool, err error) {
	_, err = io.Copy(ctx.connIn, respOut.Body)
	respOut.Body.Close()
	moreToRead = err == nil && respOut.Header.Get(X_HTTPCONN_EOF) != "true"
	return
}

// impatientIn is an io.ReadCloser that has a special Read() function which
// makes sure we don't block indefinitely on a client that has nothing new to
// send.
type impatientIn struct {
	orig         *idleTimingConn
	reader       *bufio.Reader
	idleInterval time.Duration
	pollInterval time.Duration
	hitEOF       bool // indicates that we've hit EOF on the client connection
	hasRead      bool
	startTime    time.Time
}

// Read implements the function from io.Reader.
//
// Every Read() call only waits up to idleInterval for data to be read.
// As soon as at least some data has been read, the next time it times out it
// returns EOF so that processing can continue and send whatever data was read
// to the server.
//
// The Read() function also returns io.EOF if no data has been read for more
// than pollInterval.  This ensures that we periodically poll the server to see
// if it has any more data to send us.

func (r *impatientIn) Read(b []byte) (n int, err error) {
	// Try to read from reader, but only wait idleInterval time
	deadline := time.Now().Add(r.idleInterval)
	r.orig.SetReadDeadline(deadline)
	n, err = r.reader.Read(b)
	if n > 0 {
		r.hasRead = true
	}

	if err != nil {
		switch e := err.(type) {
		case net.Error:
			if e.Timeout() {
				// Read exceeded deadline
				if r.hasRead {
					err = io.EOF
				}

				timeSinceStart := time.Now().Sub(r.startTime)
				hitPollInterval := timeSinceStart > r.pollInterval
				if hitPollInterval {
					err = io.EOF
				} else {
					err = nil
				}
			}
		default:
			if e == io.EOF {
				r.hitEOF = true
			}
		}
	}
	return
}

// Close implements Close() from io.Closer.  It does nothing, leaving the
// underlying connection open.
func (r *impatientIn) Close() error {
	return nil
}
