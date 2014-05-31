package httpconn

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	BAD_GATEWAY = 502
)

// Proxy is the server side to an http.Conn, handling incoming requests from
// the http.Conn.
type Proxy struct {
	// IdleInterval: how long to wait for the next write before finishing the
	// current HTTP response to the client
	idleInterval time.Duration

	connsOut map[string]*idleTimingConn // map of outbound connections by their id
}

func NewProxy(idleInterval time.Duration) *Proxy {
	return &Proxy{
		idleInterval: idleInterval,
		connsOut:     make(map[string]*idleTimingConn),
	}
}

func (p *Proxy) ListenAndServe(addr string) error {
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      p,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func (p *Proxy) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	connOut, err := p.connOutFor(req)
	if err != nil {
		resp.WriteHeader(BAD_GATEWAY)
		resp.Write([]byte(err.Error()))
		return
	}

	// Read request
	_, err = io.Copy(connOut, req.Body)
	if err != nil && err != io.EOF {
		log.Printf("Unexpected error reading from client: %s", err)
		connOut.Close()
	}

	// Write response
	// TODO: pool buffers and make buffer size tunable
	b := make([]byte, 8096)
	for {
		idleInterval := p.idleInterval
		if idleInterval == 0 {
			idleInterval = defaultIdleInterval
		}
		readDeadline := time.Now().Add(idleInterval)
		connOut.SetReadDeadline(readDeadline)
		n, readErr := connOut.Read(b)
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
				log.Printf("Unexpected error writing to client: %s", writeErr)
				connOut.Close()
				return
			}
		}
		if readErr != nil {
			switch e := readErr.(type) {
			case net.Error:
				if e.Timeout() {
					// Return, but leave connOut open
					return
				}
			default:
				if readErr != io.EOF {
					log.Printf("Unexpected error reading from connOut: %s", readErr)
				}
				connOut.Close()
				return
			}
		}
	}
}

func (p *Proxy) connOutFor(req *http.Request) (connOut *idleTimingConn, err error) {
	id := req.Header.Get(X_HTTPCONN_ID)
	if id == "" {
		return nil, fmt.Errorf("No id found in header %s", X_HTTPCONN_ID)
	}

	connOut = p.connsOut[id]
	if connOut == nil {
		// Connect to destination
		addr := req.Header.Get(X_HTTPCONN_DEST_ADDR)
		if addr == "" {
			return nil, fmt.Errorf("No address found in header %s", X_HTTPCONN_DEST_ADDR)
		}

		// Dial out on first request
		// TODO: make dialer pluggable
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("Unable to dial out to %s: %s", addr, err)
		}

		connOut = newIdleTimingConn(conn, defaultIdleTimeout)
		p.connsOut[id] = connOut
	}
	return
}

type idleTimingConn struct {
	conn             net.Conn
	idleTimeout      time.Duration
	lastActivityTime time.Time
}

func newIdleTimingConn(conn net.Conn, idleTimeout time.Duration) *idleTimingConn {
	c := &idleTimingConn{
		conn:             conn,
		idleTimeout:      idleTimeout,
		lastActivityTime: time.Now(),
	}
	go func() {
		for {
			time.Sleep(idleTimeout)
			if c.closeIfNecessary() {
				log.Println("Closed idle connection")
				return
			}
		}
	}()
	return c
}

func (c *idleTimingConn) Read(b []byte) (int, error) {
	c.lastActivityTime = time.Now()
	return c.conn.Read(b)
}

func (c *idleTimingConn) Write(b []byte) (int, error) {
	c.lastActivityTime = time.Now()
	return c.conn.Write(b)
}

func (c *idleTimingConn) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}

func (c *idleTimingConn) Close() error {
	return c.conn.Close()
}

func (c *idleTimingConn) closeIfNecessary() bool {
	if time.Now().Sub(c.lastActivityTime) > c.idleTimeout {
		c.Close()
		return true
	}
	return false
}

func BadGateway(w io.Writer, msg string) {
	log.Printf("Sending BadGateway: %s", msg)
	resp := &http.Response{
		StatusCode: 502,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Body:       &closeableStringReader{strings.NewReader(msg)},
	}
	resp.Write(w)
}

type closeableStringReader struct {
	*strings.Reader
}

func (r *closeableStringReader) Close() error {
	return nil
}
