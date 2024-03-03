package processor

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/events"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/watch"
)

func NewWatchEvent(e _Element) *elemwatch.Event {
	id := elemwatch.NewId(e.Id())

	evt := &elemwatch.Event{
		Node:   id,
		Lock:   string(e.GetLock()),
		Status: string(e.GetStatus()),
	}

	var links []ElementId
	state := e.GetProcessingState()
	if state != nil {
		links = state.GetLinks()
	} else {
		links = e.GetCurrentState().GetLinks()
	}

	for _, l := range links {
		evt.Links = append(evt.Links, elemwatch.NewId(l))
	}

	return evt
}

type WatchRegistry struct {
	lock           sync.Mutex
	processingMode *processingModel
	registry       *EventManager
	mappers        map[watch.EventHandler[elemwatch.Event]]*watchMapper
}

var _ watch.Registry[elemwatch.Request, elemwatch.Event] = (*WatchRegistry)(nil)

func NewWatchRegistry(m *processingModel, reg *EventManager) *WatchRegistry {
	return &WatchRegistry{
		processingMode: m,
		registry:       reg,
		mappers:        map[watch.EventHandler[elemwatch.Event]]*watchMapper{},
	}
}

func (r *WatchRegistry) RegisterHandler(req elemwatch.Request, handler watch.EventHandler[elemwatch.Event]) {
	r.lock.Lock()
	defer r.lock.Unlock()

	m := r.mappers[handler]
	if m == nil {
		m = &watchMapper{
			processingModel: r.processingMode,
			handler:         handler,
		}
		r.mappers[handler] = m
		r.registry.RegisterHandler(m, true, req.Kind, req.Namespace)
	}
	m.count++
}

func (r *WatchRegistry) UnregisterHandler(req elemwatch.Request, handler watch.EventHandler[elemwatch.Event]) {
	r.lock.Lock()
	defer r.lock.Unlock()

	m := r.mappers[handler]
	if m != nil {
		m.count--
		if m.count <= 0 {
			r.registry.UnregisterHandler(m, req.Kind, req.Namespace)
			delete(r.mappers, handler)
		}
	}
}

type watchMapper struct {
	count           int
	processingModel *processingModel
	handler         watch.EventHandler[elemwatch.Event]
}

var _ events.EventHandler[ElementId] = (*watchMapper)(nil)

func (w *watchMapper) HandleEvent(id ElementId) {
	go func() {
		e := w.processingModel._GetElement(id)
		if e != nil {
			w.handler.HandleEvent(*NewWatchEvent(e))
		}
	}()
}

////////////////////////////////////////////////////////////////////////////////

type watchEventLister struct {
	m *processingModel
}

func (l *watchEventLister) ListObjectIds(typ string, ns string, atomic ...func()) ([]elemwatch.Id, error) {
	l.m.lock.Lock()
	defer l.m.lock.Unlock()

	var list []elemwatch.Id

	if ns == "" {
		list = l.listAll(typ)
	} else {
		ni := l.m.namespaces[ns]
		ids := ni.list(typ)
		for _, id := range ids {
			list = append(list, elemwatch.NewId(id))
		}
	}

	for _, a := range atomic {
		a()
	}
	return list, nil
}

func (l *watchEventLister) listAll(typ string) []elemwatch.Id {
	var list []elemwatch.Id
	for _, ni := range l.m.namespaces {
		for _, id := range ni.list(typ) {
			list = append(list, elemwatch.NewId(id))
		}
	}
	return list
}
