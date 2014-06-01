// server.go is an example of using enproxy.NewServer
package main

import (
	"log"
	"os"

	"github.com/getlantern/enproxy"
)

func main() {
	proxy := enproxy.NewServer(0, 0)
	err := proxy.ListenAndServe(os.Args[1])
	if err != nil {
		log.Fatalf("Unable to listen and serve: %s", err)
	}
}
