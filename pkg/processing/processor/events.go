package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/events"
	"github.com/mandelsoft/engine/pkg/future"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/mandelsoft/logging"
)

type ObjectLister = events.ObjectLister[elemwatch.Event]
type EventHandler = watch.EventHandler[elemwatch.Event]
type HandlerRegistration = events.HandlerRegistration[elemwatch.Event]
type HandlerRegistrationTest = events.HandlerRegistrationTest[elemwatch.Event]

type HandlerRegistry = *handlerRegistry

var _ events.HandlerRegistry[elemwatch.Event] = (*handlerRegistry)(nil)
var _ watch.Registry[elemwatch.Request, elemwatch.Event] = (*handlerRegistry)(nil)

type handlerRegistry struct {
	events.HandlerRegistry[elemwatch.Event]
}

func (r *handlerRegistry) RegisterWatchHandler(req elemwatch.Request, h EventHandler) {
	r.RegisterHandler(h, true, req.Kind, !req.Flat, req.Namespace)
}

func (r *handlerRegistry) UnregisterWatchHandler(req elemwatch.Request, h EventHandler) {
	r.UnregisterHandler(h, req.Kind, !req.Flat, req.Namespace)
}

func newHandlerRegistry(l ObjectLister) HandlerRegistry {
	return &handlerRegistry{events.NewHandlerRegistry[elemwatch.Event](l, nil)}
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

func (p *EventManager) RegisterHandler(handler EventHandler, current bool, kind string, closure bool, ns string) {
	p.registry.RegisterHandler(handler, current, kind, closure, ns)
}

func (p *EventManager) UnregisterHandler(handler EventHandler, kind string, closure bool, ns string) {
	p.registry.UnregisterHandler(handler, kind, closure, ns)
}

func (p *EventManager) TriggerElementEvent(e _Element) {
	p.registry.TriggerEvent(*NewWatchEvent(e))
}

func (p *EventManager) TriggerNamespaceEvent(ni *namespaceInfo) {
	p.registry.TriggerEvent(*NewWatchEventForNamespace(ni))
}

func (p *EventManager) TriggerStatusEvent(log logging.Logger, e _Element) {
	p.TriggerElementEvent(e)
	p.statusEvents.Trigger(log, e.GetStatus(), e.Id())
}

func (p *EventManager) Future(etype EventType, id ElementId, retrigger ...bool) Future {
	return p.statusEvents.Future(etype, id, retrigger...)
}

func (p *EventManager) Wait(ctx context.Context, etype EventType, id ElementId) bool {
	return p.statusEvents.Wait(ctx, etype, id)
}
