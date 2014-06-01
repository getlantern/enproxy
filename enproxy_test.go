package enproxy

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	CLIENT_ADDR   = "localhost:13090"
	SERVER_ADDR   = "localhost:13091"
	EXPECTED_TEXT = "Google is built by a large team of engineers, designers, researchers, robots, and others in many different sites across the globe. It is updated continuously, and built with more tools and technologies than we can shake a stick at. If you'd like to help us out, see google.com/careers."
	HR            = "----------------------------"
)

var (
	serversStarted = false
)

func Test(t *testing.T) {
	startServers(t)

	// Single request/response pair
	doRequest(false, t)

	// Followed by another HTTP request/response pairs
	doRequest(false, t)

	// Followed by an https request
	doRequest(true, t)
}

func doRequest(https bool, t *testing.T) {
	prefix := "http"
	if https {
		prefix = "https"
	}

	resp, err := http.Get(prefix + "://www.google.com/humans.txt")
	if err != nil {
		t.Fatalf("Unable to do GET: %s", err)
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

func startServers(t *testing.T) {
	if serversStarted {
		return
	}
	go func() {
		client := &Client{
			NewRequest: func(method string, body io.ReadCloser) (*http.Request, error) {
				return http.NewRequest(method, "http://"+SERVER_ADDR, body)
			},
			HttpClient: &http.Client{},
		}
		err := client.ListenAndServe(CLIENT_ADDR)
		if err != nil {
			t.Fatalf("Unable to start client: %s", err)
		}
	}()
	waitForServer(CLIENT_ADDR, 1*time.Second, t)

	go func() {
		server := NewServer(0, 0)
		err := server.ListenAndServe(SERVER_ADDR)
		if err != nil {
			t.Fatalf("Unable to start server: %s", err)
		}
	}()
	waitForServer(SERVER_ADDR, 1*time.Second, t)

	serversStarted = true
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
