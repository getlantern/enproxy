package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/getlantern/httpconn"
)

func main() {
	client := Client{
		Addr:      os.Args[1],
		ProxyAddr: os.Args[2],
	}
	err := client.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

type Client struct {
	Addr      string
	ProxyAddr string
}

func (c *Client) ListenAndServe() error {
	listener, err := net.Listen("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("Unable to listen at address %s: %s", c.Addr, err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("Error accepting connection at address %s: %s", c.Addr, err)
			}
			return nil
		}
		go c.handleConn(conn)
	}
	return nil
}

func (c *Client) handleConn(connIn net.Conn) {
	defer connIn.Close()

	buffIn := bufio.NewReader(connIn)
	req, err := http.ReadRequest(buffIn)
	if err != nil {
		if err != io.EOF {
			httpconn.BadGateway(connIn, fmt.Sprintf("Unable to read next request: %s", err))
		}
		return
	}

	err = c.handleRequest(connIn, buffIn, req)
	if err != nil {
		httpconn.BadGateway(connIn, err.Error())
	}
}

func (c *Client) handleRequest(connIn net.Conn, buffIn *bufio.Reader, req *http.Request) error {
	// Establish outbound connection
	addr := hostIncludingPort(req)
	connOut := &httpconn.Client{
		Addr: addr,
		DialProxy: func(addr string) (net.Conn, error) {
			return net.Dial("tcp", os.Args[2])
		},
		NewRequest: func(method string, body io.Reader) (req *http.Request, err error) {
			return http.NewRequest(method, "http://"+os.Args[2]+"/", body)
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
				httpconn.BadGateway(connIn, fmt.Sprintf("Unable to write out request: %s", err))
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
