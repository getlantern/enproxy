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
func (c *Config) Intercept(resp http.ResponseWriter, req *http.Request, shouldProxyLoopback bool) {
	if req.Method != "CONNECT" {
		panic("Intercept used for non-CONNECT request!")
	}

	// Hijack underlying connection
	clientConn, buffClientConn, err := resp.(http.Hijacker).Hijack()
	if err != nil {
		resp.WriteHeader(502)
		fmt.Fprintf(resp, "Unable to hijack connection: %s", err)
	}

	addr := hostIncludingPort(req)

	// Check for local addresses, which we proxy directly
	if !shouldProxyLoopback && isLoopback(addr) {
		c.direct(clientConn, addr)
	} else {
		c.proxied(resp, req, clientConn, buffClientConn, addr)
	}
}

// direct pipes data directly to the requested address
func (c *Config) direct(clientConn net.Conn, addr string) {
	connOut, err := net.Dial("tcp", addr)
	if err != nil {
		BadGateway(clientConn, fmt.Sprintf("Unable to dial loopback address %s: %s", addr, err))
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		io.Copy(connOut, clientConn)
		wg.Done()
	}()
	go func() {
		io.Copy(clientConn, connOut)
		connOut.Close()
		wg.Done()
	}()
	wg.Wait()
}

// proxied proxies via an enproxy.Proxy
func (c *Config) proxied(resp http.ResponseWriter, req *http.Request, clientConn net.Conn, buffClientConn *bufio.ReadWriter, addr string) {
	// Establish outbound connection
	proxyConn := &Conn{
		Addr:   addr,
		Config: c,
	}
	err := proxyConn.Connect()
	if err != nil {
		BadGateway(clientConn, fmt.Sprintf("Unable to Connect to proxy: %s", err))
		return
	}
	defer proxyConn.Close()

	pipeData(clientConn, buffClientConn, proxyConn, req)
}

// pipeData pipes data between the client and proxy connections.  It's also
// responsible for responding to the initial CONNECT request with a 200 OK.
func pipeData(clientConn net.Conn, buffClientConn *bufio.ReadWriter, proxyConn *Conn, req *http.Request) {
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
		io.Copy(proxyConn, buffClientConn)
		proxyConn.Close()
	}()

	// Copy from proxy to client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, proxyConn)
		clientConn.Close()
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

func isLoopback(addr string) bool {
	ip, err := net.ResolveIPAddr("ip4", strings.Split(addr, ":")[0])
	return err == nil && ip.IP.IsLoopback()
}
