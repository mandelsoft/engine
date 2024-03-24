package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/mandelsoft/engine/pkg/service"
)

type Server struct {
	lock              sync.Mutex
	shutdownTimeout   time.Duration
	certFile, keyFile string

	server *http.Server
	mux    *http.ServeMux

	closer []io.Closer
	ready  service.Trigger
	done   service.Trigger
}

var _ service.Service = (*Server)(nil)

func NewServer(port int, def bool, shutdownTimeout time.Duration) *Server {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	if def {
		mux.Handle("/*", default_mux)
	}
	return &Server{
		server:          server,
		mux:             mux,
		shutdownTimeout: shutdownTimeout,
		done:            service.SyncTrigger(),
		ready:           service.SyncTrigger(),
	}
}

func NewTLSServer(port int, def bool, shutdownTimeout time.Duration, certFile, keyFile string) *Server {
	s := NewServer(port, def, shutdownTimeout)
	s.certFile = certFile
	s.keyFile = keyFile
	return s
}

func (s *Server) Start(ctx context.Context) (ready service.Syncher, done service.Syncher, err error) {
	go func() {
		s.listenAndServeContext(ctx)
	}()
	return s.ready, s.done, nil
}

func (s *Server) Wait() error {
	return s.done.Wait()
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)

	if c, ok := handler.(io.Closer); ok {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.closer = append(s.closer, c)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err != nil {
		return err
	}
	for _, c := range s.closer {
		err2 := c.Close()
		if err2 != nil {
			err = err2
		}
	}
	s.done.SetError(err)
	s.done.Trigger()
	return err
}

func (s *Server) listenAndServeContext(ctx context.Context) {
	serverErr := make(chan error, 1)
	go func() {
		// Capture ListenAndServe errors such as "port already in use".
		// However, when a server is gracefully shutdown, it is safe to ignore errors
		// returned from this method (given the select logic below), because
		// Shutdown causes ListenAndServe to always return http.ErrServerClosed.
		err := s.listenAndServe()
		serverErr <- err
	}()
	var err error
	select {
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		s.Shutdown(ctx)
	case err = <-serverErr:
		s.done.SetError(err)
		s.done.Trigger()
	}
	return
}

func (s *Server) listenAndServe() error {
	addr := s.server.Addr
	if addr == "" && s.certFile != "" {
		addr = ":https"
	}

	ln, err := net.Listen("tcp", addr)
	s.ready.Trigger()
	if err != nil {
		return err
	}

	defer ln.Close()

	if s.certFile == "" {
		return s.server.Serve(ln)
	}
	return s.server.ServeTLS(ln, s.certFile, s.keyFile)
}
