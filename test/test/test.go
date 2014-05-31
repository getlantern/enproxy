package main

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "fastly.getlantern.org:80")
	if err != nil {
		log.Fatalf("Unable to dial out: %s", err)
	}
	reader := bufio.NewReader(conn)
	doRequest(conn, reader)
	doRequest(conn, reader)
}

func doRequest(conn net.Conn, reader *bufio.Reader) {
	req, err := http.NewRequest("POST", "http://fastly.getlantern.org/hello", &closeableStringReader{strings.NewReader("Hello")})
	req.Header.Set("Proxy-Connection", "keep-alive")
	req.ContentLength = 5
	if err != nil {
		log.Fatalf("Unable to create request: %s", err)
	}
	err = req.Write(conn)
	if err != nil {
		log.Fatalf("Unable to write request: %s", err)
	}
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		log.Fatalf("Unable to read response: %s", err)
	}
	defer resp.Body.Close()
	log.Printf("Response: %s", resp)
}

func handle(resp http.ResponseWriter, req *http.Request) {
	req.Write(os.Stdout)
	resp.WriteHeader(500)
}

type closeableStringReader struct {
	*strings.Reader
}

func (r *closeableStringReader) Close() error {
	return nil
}
