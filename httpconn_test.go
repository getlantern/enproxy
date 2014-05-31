package httpconn

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	PROXY_ADDR    = "localhost:13091"
	EXPECTED_TEXT = "Google is built by a large team of engineers, designers, researchers, robots, and others in many different sites across the globe. It is updated continuously, and built with more tools and technologies than we can shake a stick at. If you'd like to help us out, see google.com/careers."
	HR            = "----------------------------"
)

var (
	proxyStarted = false
)

func TestPlainText(t *testing.T) {
	startProxy(t)

	conn := prepareConn(80, t)
	defer conn.Close()

	doRequests(conn, t)
}

func TestTLS(t *testing.T) {
	startProxy(t)

	conn := prepareConn(443, t)

	tlsConn := tls.Client(conn, &tls.Config{
		ServerName: "www.google.com",
	})
	defer tlsConn.Close()

	err := tlsConn.Handshake()
	if err != nil {
		t.Fatalf("Unable to handshake: %s", err)
	}

	doRequests(tlsConn, t)
}

func prepareConn(port int, t *testing.T) (conn *Client) {
	addr := fmt.Sprintf("%s:%d", "www.google.com", port)
	conn = &Client{
		Addr: addr,
		DialProxy: func(addr string) (net.Conn, error) {
			return net.Dial("tcp", PROXY_ADDR)
		},
		NewRequest: func(method string, body io.Reader) (req *http.Request, err error) {
			return http.NewRequest(method, "http://"+PROXY_ADDR, body)
		},
	}
	err := conn.Connect()
	if err != nil {
		t.Fatalf("Unable to connect: %s", err)
	}
	return
}

func doRequests(conn net.Conn, t *testing.T) {
	// Single request/response pair
	req := makeRequest(conn, t)
	readResponse(conn, req, t)

	// Consecutive request/response pairs
	req = makeRequest(conn, t)
	readResponse(conn, req, t)
}

func makeRequest(conn net.Conn, t *testing.T) *http.Request {
	req, err := http.NewRequest("GET", "http://www.google.com/humans.txt", nil)
	if err != nil {
		t.Fatalf("Unable to create request: %s", err)
	}
	req.Header.Set("Proxy-Connection", "keep-alive")
	go func() {
		err = req.Write(conn)
		if err != nil {
			t.Fatalf("Unable to write request: %s", err)
		}
	}()
	return req
}

func readResponse(conn net.Conn, req *http.Request, t *testing.T) {
	buffIn := bufio.NewReader(conn)
	resp, err := http.ReadResponse(buffIn, req)
	if err != nil {
		t.Fatalf("Unable to read response: %s", err)
	}

	buff := bytes.NewBuffer(nil)
	_, err = io.Copy(buff, resp.Body)
	if err != nil {
		t.Fatalf("Unable to read response body: %s", err)
	}
	text := string(buff.Bytes())
	if !strings.Contains(text, EXPECTED_TEXT) {
		t.Errorf("Resulting string did not contain expected text.\nExpected:\n%s\n%s\nReceived:\n%s", EXPECTED_TEXT, HR, text, HR)
	}
}

func startProxy(t *testing.T) {
	if proxyStarted {
		return
	}
	go func() {
		proxy := NewProxy(0)
		err := proxy.ListenAndServe(PROXY_ADDR)
		if err != nil {
			t.Fatalf("Unable to listen and serve: %s", err)
		}
	}()
	waitForServer(PROXY_ADDR, 1*time.Second, t)
	proxyStarted = true
}

// waitForServer waits for a TCP server to start at the given address, waiting
// up to the given limit and reporting an error to the given testing.T if the
// server didn't start within the time limit.
func waitForServer(addr string, limit time.Duration, t *testing.T) {
	cutoff := time.Now().Add(limit)
	for {
		if time.Now().After(cutoff) {
			t.Errorf("Server never came up at address %s", addr)
			return
		}
		c, err := net.DialTimeout("tcp", addr, limit)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
