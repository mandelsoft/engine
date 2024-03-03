package watch

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type EventHandler[E any] interface {
	Handle(e E)
}

type Register[R any, E any] interface {
	Register(r R, h EventHandler[E])
}

func WatchRequest[R, E any](r Register[R, E]) http.Handler {
	return &RequestHandler[R, E]{r}
}

type RequestHandler[R, E any] struct {
	register Register[R, E]
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
		wsutil.WriteServerMessage(conn, ws.OpBinary, (&Error{err.Error()}).Data())
	}
	if op != ws.OpBinary {
		log.Error("no binary data")
		wsutil.WriteServerMessage(conn, ws.OpBinary, (&Error{"binary registration request required"}).Data())
	}

	var registration R

	err = json.Unmarshal(msg, &registration)
	if err != nil {
		log.LogError(err, "decoding registration request")
		wsutil.WriteServerMessage(conn, op, (&Error{err.Error()}).Data())
	}

	log.Info("registering handler for request")
	handler := NewHandler[E](conn)
	h.register.Register(registration, handler)
}

type Handler[E any] struct {
	conn net.Conn
}

func NewHandler[E any](conn net.Conn) *Handler[E] {
	return &Handler[E]{conn}
}

func (h *Handler[E]) Handle(e E) {
	log.Info("sending event {{event}}", "event", e)
	data, _ := json.Marshal(e)
	err := wsutil.WriteServerMessage(h.conn, ws.OpBinary, data)
	if err != nil {
		log.LogError(err, "cannot send event")
	}
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
