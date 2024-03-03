package watch

import (
	"time"

	"github.com/mandelsoft/engine/pkg/server"
	"github.com/mandelsoft/engine/watch"
)

func NewServer(port int, pattern string, reg Registry) *server.Server {
	srv := server.NewServer(port, false, 10*time.Second)
	srv.Handle(pattern, watch.WatchHttpHandler[Request, Event](reg))
	return srv
}
