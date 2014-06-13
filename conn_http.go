package enproxy

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
)

// Intercept intercepts a CONNECT request, hijacks the underlying client
// connetion and starts piping the data over a new enproxy.Conn configured using
// this Config.
func (c *Config) Intercept(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		panic("Intercept used for non-CONNECT request!")
	}

	// Hijack underlying connection
	clientConn, buffClientConn, err := resp.(http.Hijacker).Hijack()
	if err != nil {
		resp.WriteHeader(502)
		fmt.Fprintf(resp, "Unable to hijack connection: %s", err)
	}
	defer clientConn.Close()

	addr := hostIncludingPort(req)
	c.proxied(resp, req, clientConn, buffClientConn, addr)
}

// proxied proxies via an enproxy.Proxy
func (c *Config) proxied(resp http.ResponseWriter, req *http.Request, clientConn net.Conn, buffClientConn *bufio.ReadWriter, addr string) {
	// Establish outbound connection
	connOut := &Conn{
		Addr:   addr,
		Config: c,
	}
	connOut.Connect()
	defer connOut.Close()

	pipeData(clientConn, buffClientConn, connOut, req)
}

// pipeData pipes data between the client and proxy connections.  It's also
// responsible for responding to the initial CONNECT request with a 200 OK.
func pipeData(clientConn net.Conn, buffClientConn *bufio.ReadWriter, connOut *Conn, req *http.Request) {
	// Pipe data between inbound and outbound connections
	var wg sync.WaitGroup
	wg.Add(2)

	// Respond OK and copy from client to proxy
	go func() {
		defer wg.Done()
		err := respondOK(clientConn, req)
		if err != nil {
			log.Printf("Unable to respond OK: %s", err)
			return
		}
		io.Copy(connOut, buffClientConn)
	}()

	// Copy from proxy to client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, connOut)
	}()
	wg.Wait()
}

func respondOK(clientConn net.Conn, req *http.Request) error {
	defer req.Body.Close()
	resp := &http.Response{
		StatusCode: 200,
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	return resp.Write(clientConn)
}

func hostIncludingPort(req *http.Request) string {
	parts := strings.Split(req.Host, ":")
	if len(parts) == 1 {
		return req.Host + ":443"
	} else {
		return req.Host
	}
}
