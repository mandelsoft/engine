package future

import (
	"context"
	"sync"

	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

////////////////////////////////////////////////////////////////////////////////

type waiting[I comparable] map[I][]*future

type EventManager[I comparable, E comparable] interface {
	Trigger(log logging.Logger, e E, id I)
	Wait(ctx context.Context, e E, id I) bool

	Future(e E, id I, retrigger ...bool) Future
}

type eventManager[I comparable, E comparable] struct {
	lock  sync.Mutex
	types map[E]waiting[I]
}

func NewEventManager[I comparable, E comparable]() EventManager[I, E] {
	return &eventManager[I, E]{
		types: map[E]waiting[I]{},
	}
}

func (p *eventManager[I, E]) Trigger(log logging.Logger, e E, id I) {
	if log != nil {
		log.Debug("trigger event {{event}} for {{target}}", "event", e, "target", id)
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.types[e]
	if state == nil {
		state = waiting[I]{}
		p.types[e] = state
	}

	waiting := state[id]
	if waiting != nil {
		var n []*future
		for _, w := range waiting {
			if w.retrigger {
				n = append(n, w)
			}
			w.Trigger()
		}
		state[id] = n
	}
}

func (p *eventManager[I, E]) Future(e E, id I, retrigger ...bool) Future {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.types[e]
	if state == nil {
		state = waiting[I]{}
		p.types[e] = state
	}

	f := NewFuture(utils.Optional(retrigger...))
	state[id] = append(state[id], f)
	return f
}

func (p *eventManager[I, E]) Wait(ctx context.Context, e E, id I) bool {
	return p.Future(e, id).Wait(ctx)
}
