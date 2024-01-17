package processing

import (
	"github.com/mandelsoft/engine/pkg/database"
)

var _ database.EventHandler = (*Handler)(nil)

type Handler struct {
	proc *Processor
}

func newHandler(p *Processor) database.EventHandler {
	return &Handler{
		p,
	}
}

func (h *Handler) HandleEvent(id database.ObjectId) {
	// TODO implement me
	panic("implement me")
}
