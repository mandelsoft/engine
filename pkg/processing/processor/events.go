package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/events"
	"github.com/mandelsoft/engine/pkg/future"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

type ObjectLister = events.ObjectLister[ElementId]
type EventHandler = events.EventHandler[ElementId]
type HandlerRegistration = events.HandlerRegistration[ElementId]
type HandlerRegistrationTest = events.HandlerRegistrationTest[ElementId]
type HandlerRegistry = events.HandlerRegistry[ElementId]

func newHandlerRegistry(l ObjectLister) HandlerRegistry {
	return events.NewHandlerRegistry[ElementId](l, nil)
}

////////////////////////////////////////////////////////////////////////////////

type PendingCounter struct {
	lock    sync.Mutex
	pending int64
	waiting []chan struct{}
}

func (p *PendingCounter) Add(delta int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.pending += delta

	fmt.Printf("******* pending %d (%d) *******\n", p.pending, len(p.waiting))
	if p.pending <= 0 {
		p.pending = 0
		for _, w := range p.waiting {
			close(w)
		}
		p.waiting = nil
	}
}

func (p *PendingCounter) Wait(ctx context.Context) bool {
	p.lock.Lock()

	c := make(chan struct{})
	p.waiting = append(p.waiting, c)
	fmt.Printf("******* waiting %d (%d) *******\n", p.pending, len(p.waiting))
	p.lock.Unlock()

	defer func() {
		p.lock.Lock()
		for i, w := range p.waiting {
			if w == c {
				p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
				break
			}
		}
		p.lock.Unlock()
	}()
	select {
	case <-c:
		return true
	case <-ctx.Done():
		return false
	}
}

////////////////////////////////////////////////////////////////////////////////

type EventType = model.Status
type Future = future.Future

var _ ObjectLister = (*lister)(nil)

type EventManager struct {
	lock         sync.Mutex
	statusEvents future.EventManager[ElementId, EventType]
	registry     HandlerRegistry
}

func newEventManager(proc *processingModel) *EventManager {
	return &EventManager{
		statusEvents: future.NewEventManager[ElementId, EventType](),
		registry:     newHandlerRegistry(proc.lister()),
	}
}

func (p *EventManager) RegisterHandler(handler EventHandler, current bool, kind string, nss ...string) {
	p.registry.RegisterHandler(handler, current, kind, nss...)
}

func (p *EventManager) UnregisterHandler(handler EventHandler, kind string, nss ...string) {
	p.registry.UnregisterHandler(handler, kind, nss...)
}

func (p *EventManager) TriggerElementHandled(id ElementId) {
	p.registry.TriggerEvent(id)
}

func (p *EventManager) TriggerStatusEvent(log logging.Logger, etype EventType, id ElementId) {
	p.statusEvents.Trigger(log, etype, id)
}

func (p *EventManager) Future(etype EventType, id ElementId, retrigger ...bool) Future {
	return p.statusEvents.Future(etype, id, retrigger...)
}

func (p *EventManager) Wait(ctx context.Context, etype EventType, id ElementId) bool {
	return p.statusEvents.Wait(ctx, etype, id)
}
