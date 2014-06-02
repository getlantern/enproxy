package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"sync"

	"github.com/getlantern/enproxy"
)

func main() {
	httpServer := &http.Server{
		Addr: os.Args[1],
		Handler: &ClientHandler{
			ProxyAddr: os.Args[2],
			ReverseProxy: &httputil.ReverseProxy{
				Director: func(req *http.Request) {
					// do nothing
				},
			},
		},
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

type ClientHandler struct {
	ProxyAddr    string
	ReverseProxy *httputil.ReverseProxy
}

func (c *ClientHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method == "CONNECT" {
		connIn, buffIn, err := resp.(http.Hijacker).Hijack()
		if err != nil {
			resp.WriteHeader(502)
			fmt.Fprintf(resp, "Unable to hijack connection: %s", err)
		}
		c.handleRequest(connIn, buffIn, req)
	} else {
		c.ReverseProxy.ServeHTTP(resp, req)
	}
}

func (c *ClientHandler) handleRequest(connIn net.Conn, buffIn *bufio.ReadWriter, req *http.Request) error {
	// Establish outbound connection
	addr := hostIncludingPort(req)
	connOut := &enproxy.Client{
		Addr: addr,
		Config: &enproxy.Config{
			DialProxy: func(addr string) (net.Conn, error) {
				return net.Dial("tcp", os.Args[2])
			},
			NewRequest: func(method string, body io.Reader) (req *http.Request, err error) {
				return http.NewRequest(method, "http://"+os.Args[2]+"/", body)
			},
		},
	}
	defer connOut.Close()

	err := connOut.Connect()
	if err != nil {
		return fmt.Errorf("Unable to dial proxy: %s", err)
	}
	defer connOut.Close()

	// Pipe data between inbound and outbound connections
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if req.Method != "CONNECT" {
			err = req.Write(connOut)
			if err != nil {
				enproxy.BadGateway(connIn, fmt.Sprintf("Unable to write out request: %s", err))
			}
		} else {
			resp := &http.Response{
				StatusCode: 200,
				ProtoMajor: 1,
				ProtoMinor: 1,
			}
			resp.Write(connIn)
			req.Body.Close()
		}

		io.Copy(connOut, buffIn)
		connOut.Close()
	}()
	go func() {
		defer wg.Done()
		io.Copy(connIn, connOut)
		connIn.Close()
	}()
	wg.Wait()
	return nil
}

func hostIncludingPort(req *http.Request) string {
	parts := strings.Split(req.Host, ":")
	if len(parts) == 1 {
		return req.Host + ":80"
	} else {
		return req.Host
	}
}
