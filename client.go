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

const (
	CONNECT = "CONNECT"
)

type NewRequestFunc func(method string, body io.ReadCloser) (*http.Request, error)

// Client is a net.Conn that tunnels its data via an httpconn.Proxy using HTTP
// requests and responses.  It assumes that streaming requests are not supported
// by the underlying servers/proxies, and so uses a polling technique similar to
// the one used by meek, but different in that data is not encoded as JSON.
// https://trac.torproject.org/projects/tor/wiki/doc/AChildsGardenOfPluggableTransports#Undertheencryption.
//
// The basics flow is as follows:
//   1. Accept writes, piping these to the proxy as the body of an http request
//   2. Continue to pipe the writes until the pause between consecutive writes
//      exceeds the IdleInterval, at which point we finish the request body
//   3. Accept reads, reading the data from the response body until EOF is
//      is reached or the gap between consecutive reads exceeds the
//      IdleInterval. If EOF wasn't reached, whenever we next accept reads, we
//      will continue to read from the same response until EOF is reached, then
//      move on to the next response.
//   4. Go back to accepting writes (step 1)
//   5. If no writes are received for more than PollInterval, issue an empty
//      request in order to pick up any new data received on the proxy, start
//      accepting reads (step 3)
//
type Client struct {
	// NewRequest: function to create a new request to the proxy
	NewRequest NewRequestFunc

	// HttpClient: client used to send encapsulated HTTP requests
	HttpClient *http.Client

	// IdleTimeout: how long to wait before closing an idle inbound connection
	IdleTimeout time.Duration
}

func (c *Client) ListenAndServe(addr string) error {
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
		go c.handleConn(conn)
	}
}

func (c *Client) handleConn(conn net.Conn) {
	defer conn.Close()

	// Add idle timing to conn
	connIn := newIdleTimingConn(conn, defaultIdleTimeout)
	// Set up a buffered reader
	reader := bufio.NewReader(connIn)

	// Set up a globally unique connection id
	connId := uuid.NewRandom().String()
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			log.Printf("Error reading request: %s", err)
			return
		}
		if !c.handleRequest(connIn, reader, connId, req) {
			return
		}
	}

}

func (c *Client) handleRequest(connIn *idleTimingConn, reader *bufio.Reader, connId string, req *http.Request) bool {
	isConnect := req.Method == CONNECT
	addr := hostIncludingPort(req, isConnect)

	var reqOut *http.Request
	var respOut *http.Response
	var err error

	if !isConnect {
		requestBody, requestBodyWriter := io.Pipe()
		go func() {
			req.Write(requestBodyWriter)
			requestBodyWriter.Close()
		}()

		// Write out initial request
		reqOut, err = c.buildRequestOut(connId, addr, requestBody)
		if err != nil {
			BadGateway(connIn, fmt.Sprintf("Unable to construct outbound request: %s", err))
			return false
		}

		respOut, err = c.HttpClient.Do(reqOut)
		if err != nil {
			BadGateway(connIn, err.Error())
			return false
		}
		if respOut.StatusCode != 200 {
			BadGateway(connIn, fmt.Sprintf("Unexpected response status from proxy: %d", respOut.StatusCode))
			return false
		}
	} else {
		OK(connIn)
	}

	if !isConnect {
		moreToRead, err := copyResponse(connIn, respOut)
		if !moreToRead {
			return err == nil
		}
	}

	for {
		// Still have more data to process
		ireader := &impatientReadCloser{
			orig:        connIn,
			reader:      reader,
			idleTimeout: defaultIdleInterval,
			maxIdleTime: defaultPollInterval,
			startTime:   time.Now(),
		}
		reqOut, err = c.buildRequestOut(connId, addr, ireader)
		if err != nil {
			return false
		}
		respOut, err = c.HttpClient.Do(reqOut)
		if err != nil || respOut.StatusCode != 200 {
			return false
		}
		if ireader.hitEOF {
			// connIn must be closed, stop processing
			return false
		}
		moreToRead, err := copyResponse(connIn, respOut)
		if !moreToRead {
			return err == nil
		}
	}
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

func (c *Client) buildRequestOut(connId string, addr string, requestBody io.ReadCloser) (*http.Request, error) {
	reqOut, err := c.NewRequest("POST", requestBody)
	if err == nil {
		// Set our ID
		reqOut.Header.Set(X_HTTPCONN_ID, connId)
		// Set the address that we're trying to reach
		reqOut.Header.Set(X_HTTPCONN_DEST_ADDR, addr)
	}
	return reqOut, err
}

func copyResponse(connIn *idleTimingConn, respOut *http.Response) (moreToRead bool, err error) {
	_, err = io.Copy(connIn, respOut.Body)
	respOut.Body.Close()
	moreToRead = err == nil && respOut.Header.Get(X_HTTPCONN_EOF) != "true"
	return
}

type impatientReadCloser struct {
	orig        *idleTimingConn
	reader      *bufio.Reader
	idleTimeout time.Duration
	maxIdleTime time.Duration
	hitEOF      bool
	hasRead     bool
	startTime   time.Time
}

func (r *impatientReadCloser) Read(b []byte) (n int, err error) {
	deadline := time.Now().Add(r.idleTimeout)
	r.orig.SetReadDeadline(deadline)
	n, err = r.reader.Read(b)
	if n > 0 {
		r.hasRead = true
	}
	if err != nil {
		switch e := err.(type) {
		case net.Error:
			if e.Timeout() {
				timeSinceStart := time.Now().Sub(r.startTime)
				maxIdleTimeExceeded := timeSinceStart > r.maxIdleTime
				if r.hasRead || maxIdleTimeExceeded {
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
func (r *impatientReadCloser) Close() error {
	return nil
}
