package processing

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
)

var _ database.EventHandler = (*Handler)(nil)

type Handler struct {
	pool pool.Pool
}

func newHandler(p pool.Pool) database.EventHandler {
	return &Handler{
		p,
	}
}

func (h *Handler) HandleEvent(id database.ObjectId) {
	h.pool.EnqueueKey(id)
}
