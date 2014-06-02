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

// Intercept intercepts a request/response pair and starts piping the data over
// a new enproxy.Conn
func (c *Config) Intercept(resp http.ResponseWriter, req *http.Request) {
	clientConn, buffClientConn, err := resp.(http.Hijacker).Hijack()
	if err != nil {
		resp.WriteHeader(502)
		fmt.Fprintf(resp, "Unable to hijack connection: %s", err)
	}

	// Establish outbound connection
	addr := hostIncludingPort(req)
	proxyConn := &Conn{
		Addr:   addr,
		Config: c,
	}
	err = proxyConn.Connect()
	if err != nil {
		BadGateway(clientConn, fmt.Sprintf("Unable to dial proxy: %s", err))
		return
	}
	defer proxyConn.Close()

	pipeData(clientConn, buffClientConn, proxyConn, req)
}

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
		return req.Host + ":80"
	} else {
		return req.Host
	}
}
