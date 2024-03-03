package server

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	*http.Server
	*http.ServeMux
}

func NewServer(port int, def bool) *Server {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	if def {
		mux.Handle("/", default_mux)
	}
	return &Server{
		Server:   server,
		ServeMux: mux,
	}
}

func (s *Server) ListenAndServeContext(ctx context.Context, shutdownTimeout time.Duration) error {
	return s.ListenAndServeTLSContext(ctx, shutdownTimeout, "", "")
}

func (s *Server) ListenAndServeTLSContext(ctx context.Context, shutdownTimeout time.Duration, certFile, keyFile string) error {
	serverErr := make(chan error, 1)
	go func() {
		// Capture ListenAndServe errors such as "port already in use".
		// However, when a server is gracefully shutdown, it is safe to ignore errors
		// returned from this method (given the select logic below), because
		// Shutdown causes ListenAndServe to always return http.ErrServerClosed.
		if certFile != "" && keyFile != "" {
			serverErr <- s.ListenAndServeTLS(certFile, keyFile)
		} else {
			serverErr <- s.ListenAndServe()
		}
	}()
	var err error
	select {
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		err = s.Shutdown(ctx)
	case err = <-serverErr:
	}
	return err
}
