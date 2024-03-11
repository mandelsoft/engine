package controllers

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

type Handler struct {
	lock sync.RWMutex

	c *ExpressionController

	usage map[database.ObjectId]database.ObjectId
}

var _ database.EventHandler = (*Handler)(nil)

func NewHandler(c *ExpressionController) *Handler {
	return &Handler{
		c:     c,
		usage: map[database.ObjectId]database.ObjectId{},
	}
}

func (h *Handler) Register() {
	h.c.db.RegisterHandler(h, true, mymetamodel.TYPE_EXPRESSION, true, "")

	h.c.db.RegisterHandler(h, false, mymetamodel.TYPE_VALUE, true, "")
	h.c.db.RegisterHandler(h, false, mymetamodel.TYPE_OPERATOR, true, "")
}

func (h *Handler) HandleEvent(id database.ObjectId) {
	id = database.NewObjectIdFor(id)

	if id.GetType() == mymetamodel.TYPE_EXPRESSION {
		h.c.pool.EnqueueKey(id)
	} else {
		h.lock.RLock()
		defer h.lock.RUnlock()
		tgt := h.usage[id]
		if tgt != nil {
			h.c.pool.EnqueueKey(tgt)
		}
	}
}

func (h *Handler) Use(src, tgt database.ObjectId) bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	src = database.NewObjectIdFor(src)

	if t := h.usage[src]; t != nil && database.CompareObjectId(t, tgt) == 0 {
		return false
	}
	h.usage[src] = database.NewObjectIdFor(tgt)
	return true
}

func (h *Handler) Unuse(src database.ObjectId) bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	src = database.NewObjectIdFor(src)
	if t := h.usage[src]; t == nil {
		return false
	}
	delete(h.usage, database.NewObjectIdFor(src))
	return true
}
