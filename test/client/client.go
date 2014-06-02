package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"

	"github.com/getlantern/enproxy"
)

func main() {
	httpServer := &http.Server{
		Addr: os.Args[1],
		Handler: &ClientHandler{
			ProxyAddr: os.Args[2],
			Config: &enproxy.Config{
				DialProxy: func(addr string) (net.Conn, error) {
					return net.Dial("tcp", os.Args[2])
				},
				NewRequest: func(method string, body io.Reader) (req *http.Request, err error) {
					return http.NewRequest(method, "http://"+os.Args[2]+"/", body)
				},
			},
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
	Config       *enproxy.Config
	ReverseProxy *httputil.ReverseProxy
}

func (c *ClientHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method == "CONNECT" {
		c.Config.Intercept(resp, req)
	} else {
		c.ReverseProxy.ServeHTTP(resp, req)
	}
}
