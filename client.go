package enproxy

import (
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
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      c,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func (c *Client) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	connId := uuid.NewRandom().String()
	addr := hostIncludingPort(req)

	requestBody, requestBodyWriter := io.Pipe()
	go func() {
		log.Printf("URI: %s", req.URL.RequestURI())
		req.Write(requestBodyWriter)
		requestBodyWriter.Close()
	}()

	var reqOut *http.Request
	var respOut *http.Response
	var err error

	if req.Method != "CONNECT" {
		// Write out initial request
		reqOut, err = c.buildRequestOut(connId, addr, requestBody)
		if err != nil {
			resp.WriteHeader(502)
			fmt.Fprintf(resp, "Unable to construct outbound request: %s", err)
			return
		}

		respOut, err = c.HttpClient.Do(reqOut)
		if err != nil {
			resp.WriteHeader(502)
			resp.Write([]byte(err.Error()))
			return
		}
		if respOut.StatusCode != 200 {
			resp.WriteHeader(502)
			fmt.Fprintf(resp, "Unexpected response status from proxy: %d", respOut.StatusCode)
			return
		}
	} else {
		resp.WriteHeader(200)
	}

	// Hijack inbound connection
	_connIn, _, err := resp.(http.Hijacker).Hijack()
	if err != nil {
		resp.WriteHeader(502)
		resp.Write([]byte(err.Error()))
		return
	}
	// Add idle timing to connIn
	connIn := newIdleTimingConn(_connIn, defaultIdleTimeout)

	if req.Method != "CONNECT" {
		if !copyResponse(connIn, respOut) {
			return
		}
	}

	for {
		// Still have more data to process
		ireader := &impatientReadCloser{
			orig:        connIn,
			idleTimeout: defaultIdleInterval,
			maxIdleTime: defaultPollInterval,
			startTime:   time.Now(),
		}
		reqOut, err = c.buildRequestOut(connId, addr, ireader)
		if err != nil {
			return
		}
		respOut, err = c.HttpClient.Do(reqOut)
		done := err != nil || respOut.StatusCode != 200 || !copyResponse(connIn, respOut)
		if done {
			return
		}
	}
}

func hostIncludingPort(req *http.Request) string {
	parts := strings.Split(req.Host, ":")
	if len(parts) == 1 {
		return req.Host + ":80"
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

func copyResponse(connIn *idleTimingConn, respOut *http.Response) (moreToRead bool) {
	io.Copy(connIn, respOut.Body)
	respOut.Body.Close()
	return respOut.Header.Get(X_HTTPCONN_EOF) != "true"
}

type impatientReadCloser struct {
	orig        *idleTimingConn
	idleTimeout time.Duration
	maxIdleTime time.Duration
	hasRead     bool
	startTime   time.Time
}

func (r *impatientReadCloser) Read(b []byte) (n int, err error) {
	deadline := time.Now().Add(r.idleTimeout)
	r.orig.SetReadDeadline(deadline)
	n, err = r.orig.Read(b)
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
		}
	}
	return
}

// Close implements Close() from io.Closer.  It does nothing, leaving the
// underlying connection open.
func (r *impatientReadCloser) Close() error {
	return nil
}
