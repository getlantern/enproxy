package enproxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
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
	clientConn, bufClientConn, err := resp.(http.Hijacker).Hijack()
	if err != nil {
		resp.WriteHeader(502)
		fmt.Fprintf(resp, "Unable to hijack connection: %s", err)
	}
	defer clientConn.Close()

	addr := hostIncludingPort(req, 443)

	// Establish outbound connection
	connOut := &Conn{
		Addr:   addr,
		Config: c,
	}
	connOut.Connect()
	defer connOut.Close()

	// Respond OK and flush
	err = respondOK(bufClientConn, req)
	if err != nil {
		log.Printf("Unable to respond OK: %s", err)
		return
	}
	bufClientConn.Writer.Flush()

	// Pipe data
	pipeData(clientConn, connOut, req)
}

// pipeData pipes data between the client and proxy connections.  It's also
// responsible for responding to the initial CONNECT request with a 200 OK.
func pipeData(clientConn net.Conn, connOut *Conn, req *http.Request) {
	// Pipe data between inbound and outbound connections
	var wg sync.WaitGroup
	wg.Add(1)

	// Start piping to proxy
	go func() {
		defer wg.Done()
		io.Copy(connOut, clientConn)
	}()

	// Then start coyping from out to writer
	io.Copy(clientConn, connOut)

	wg.Wait()
}

func respondOK(writer io.Writer, req *http.Request) error {
	defer req.Body.Close()
	resp := &http.Response{
		StatusCode: 200,
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	return resp.Write(writer)
}

// hostIncludingPort extracts the host:port from a request.  It fills in a
// a default port if none was found in the request.
func hostIncludingPort(req *http.Request, defaultPort int) string {
	parts := strings.Split(req.Host, ":")
	if len(parts) == 1 {
		return req.Host + ":" + strconv.Itoa(defaultPort)
	} else {
		return req.Host
	}
}
