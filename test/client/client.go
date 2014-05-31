package main

import (
	"io"
	"log"
	"net/http"
	"os"

	"github.com/getlantern/enproxy"
)

func main() {
	client := &enproxy.Client{
		NewRequest: func(method string, body io.ReadCloser) (*http.Request, error) {
			return http.NewRequest(method, "http://"+os.Args[2], body)
		},
		HttpClient: &http.Client{},
	}
	err := client.ListenAndServe(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
}
