package enproxy

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	BAD_GATEWAY         = 502
	DEFAULT_BUFFER_SIZE = 8096
)

// Server is the server side to an enproxy.Client.  Server implements the
// http.Hander interface for plugging into an HTTP server, and it also provides
// a convenience ListenAndServe() function for quickly starting up a dedicated
// HTTP server using this Server as its handler.
type Server struct {
	idleInterval time.Duration

	bufferSize int

	connMap map[string]*idleTimingConn // map of outbound connections by their id
}

// NewServer sets up a new server.
//
// idleInterval controls how long to wait for the next write before finishing
// the current HTTP response to the client.
//
// bufferSize controls the size of the buffers used for copying data from
// outbound to inbound connection.  If given as 0, defaults to
// DEFAULT_BUFFER_SIZE bytes.
func NewServer(idleInterval time.Duration, bufferSize int) *Server {
	if bufferSize == 0 {
		bufferSize = DEFAULT_BUFFER_SIZE
	}
	return &Server{
		idleInterval: idleInterval,
		bufferSize:   bufferSize,
		connMap:      make(map[string]*idleTimingConn),
	}
}

// ListenAndServe: convenience function for quickly starting up a dedicated HTTP
// server using this Server as its handler.
func (s *Server) ListenAndServe(addr string) error {
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      s,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return httpServer.ListenAndServe()
}

// ServeHTTP: implements the http.Handler interface.
func (s *Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	connOut, err := s.connOutFor(req)
	if err != nil {
		resp.WriteHeader(BAD_GATEWAY)
		resp.Write([]byte(err.Error()))
		return
	}

	// Read request
	_, err = io.Copy(connOut, req.Body)
	if err != nil && err != io.EOF {
		resp.WriteHeader(BAD_GATEWAY)
		resp.Write([]byte(err.Error()))
		connOut.Close()
	}

	// Write response
	b := make([]byte, s.bufferSize)
	for {
		idleInterval := s.idleInterval
		if idleInterval == 0 {
			idleInterval = defaultIdleInterval
		}
		readDeadline := time.Now().Add(idleInterval)
		connOut.SetReadDeadline(readDeadline)
		n, readErr := connOut.Read(b)
		if readErr == io.EOF {
			// Reached EOF
			resp.Header().Set(X_HTTPCONN_EOF, "true")
		}
		if n > 0 {
			_, writeErr := resp.Write(b[:n])
			if writeErr != nil {
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
				connOut.Close()
				return
			}
		}
	}
}

// connOutFor opens an outbound connection to the destination requested by the
// given http.Request.
func (s *Server) connOutFor(req *http.Request) (connOut *idleTimingConn, err error) {
	id := req.Header.Get(X_HTTPCONN_ID)
	if id == "" {
		return nil, fmt.Errorf("No id found in header %s", X_HTTPCONN_ID)
	}

	connOut = s.connMap[id]
	if connOut == nil {
		// Connect to destination
		addr := req.Header.Get(X_HTTPCONN_DEST_ADDR)
		if addr == "" {
			return nil, fmt.Errorf("No address found in header %s", X_HTTPCONN_DEST_ADDR)
		}

		// Dial out on first request
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("Unable to dial out to %s: %s", addr, err)
		}

		// Wrap the connection in an idle timing one
		connOut = newIdleTimingConn(conn, defaultIdleTimeout, func() {
			delete(s.connMap, id)
		})

		s.connMap[id] = connOut
	}
	return
}
