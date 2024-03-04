package watch

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/mandelsoft/engine/pkg/utils"
)

type EventHandler[E any] interface {
	HandleEvent(e E)
}

type Registry[R any, E any] interface {
	RegisterWatchHandler(r R, h EventHandler[E])
	UnregisterWatchHandler(r R, h EventHandler[E])
}

func WatchHttpHandler[R, E any](r Registry[R, E]) *RequestHandler[R, E] {
	return &RequestHandler[R, E]{registry: r}
}

type RequestHandler[R, E any] struct {
	lock        sync.Mutex
	registry    Registry[R, E]
	connections []*handler[R, E]
}

var _ http.Handler = (*RequestHandler[any, any])(nil)

func (h *RequestHandler[R, E]) Close() error {
	h.lock.Lock()
	conns := slices.Clone(h.connections)
	h.lock.Unlock()

	for _, c := range conns {
		c.Close()
	}
	return nil
}

func (h *RequestHandler[R, E]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("new watch request")
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		fmt.Fprintf(w, "%s", (&Error{err.Error()}).Message())
	}

	msg, op, err := wsutil.ReadClientData(conn)
	if err != nil {
		log.LogError(err, "reading registration request")
		wsutil.WriteServerMessage(conn, ws.OpText, (&Error{err.Error()}).Data())
	}
	if op != ws.OpText {
		log.Error("no binary data")
		wsutil.WriteServerMessage(conn, ws.OpText, (&Error{"binary registration request required"}).Data())
	}

	var registration R

	err = json.Unmarshal(msg, &registration)
	if err != nil {
		log.LogError(err, "decoding registration request")
		wsutil.WriteServerMessage(conn, op, (&Error{err.Error()}).Data())
	}

	newHandler[R, E](h, conn, h.registry, registration)
}

func (h *RequestHandler[R, E]) addHandler(c *handler[R, E]) {
	log.Info("registering watch handler for request")
	h.lock.Lock()
	defer h.lock.Unlock()
	h.connections = append(h.connections, c)
}

func (h *RequestHandler[R, E]) removeHandler(c *handler[R, E]) {
	log.Info("unregistering watch handler")
	h.lock.Lock()
	defer h.lock.Unlock()
	h.connections = utils.FilterSlice(h.connections, utils.NotFilter(utils.EqualsFilter(c)))
}

////////////////////////////////////////////////////////////////////////////////

type handler[R, E any] struct {
	hhandler *RequestHandler[R, E]
	conn     net.Conn
	req      R
	registry Registry[R, E]
}

func newHandler[R, E any](hh *RequestHandler[R, E], conn net.Conn, registry Registry[R, E], req R) *handler[R, E] {
	h := &handler[R, E]{hh, conn, req, registry}
	registry.RegisterWatchHandler(req, h)
	hh.addHandler(h)
	return h
}

func (h *handler[R, E]) HandleEvent(e E) {
	log.Info("sending event {{event}}", "event", e)
	data, _ := json.Marshal(e)
	err := wsutil.WriteServerMessage(h.conn, ws.OpText, data)
	if err != nil {
		log.LogError(err, "cannot send event -> closing connection")
		h.conn.Close()
		h.registry.UnregisterWatchHandler(h.req, h)
	}
}
func (h *handler[R, E]) Close() error {
	log.Info("closing connection and unregister handler for {{req}}", "req", h.req)
	h.conn.Close()
	h.registry.UnregisterWatchHandler(h.req, h)
	h.hhandler.removeHandler(h)
	return nil
}

type Error struct {
	Error string `json:error"`
}

func (e *Error) Message() string {
	return string(e.Data())
}

func (e *Error) Data() []byte {
	data, _ := json.Marshal(e)
	return data
}
