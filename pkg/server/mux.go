package server

import (
	"net/http"
)

var default_mux = http.NewServeMux()

func Register(pattern string, handler http.Handler) {
	default_mux.Handle(pattern, handler)
}
