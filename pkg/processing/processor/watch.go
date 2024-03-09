package processor

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
)

func NewWatchEventForNamespace(ni *namespaceInfo) *elemwatch.Event {
	id := elemwatch.NewId(NewElementIdForPhase(ni.namespace, ""))
	lock := string(ni.namespace.GetLock())
	status := "Ready"
	if lock != "" {
		status = "Locked"
	}
	evt := &elemwatch.Event{
		Node:   id,
		Lock:   lock,
		Status: status,
	}
	return evt
}

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

////////////////////////////////////////////////////////////////////////////////

type watchEventLister struct {
	m *processingModel
}

func (l *watchEventLister) ListObjectIds(typ string, ns string, atomic ...func()) ([]elemwatch.Event, error) {
	l.m.lock.Lock()
	defer l.m.lock.Unlock()

	var list []elemwatch.Event

	if ns == "" {
		list = l.listAll(typ)
	} else {
		ni := l.m.namespaces[ns]
		ids := ni.list(typ)
		for _, id := range ids {
			e := l.m._GetElement(id)
			if e != nil {
				list = append(list, *NewWatchEvent(e))
			}
		}
	}

	for _, a := range atomic {
		a()
	}
	return list, nil
}

func (l *watchEventLister) listAll(typ string) []elemwatch.Event {
	var list []elemwatch.Event

	if typ == "" || typ == l.m.mm.NamespaceType() {
		list = append(list, *NewWatchEventForNamespace(l.m.namespaces[""]))
	}

	for _, ni := range l.m.namespaces {
		if typ == "" || typ == l.m.mm.NamespaceType() {
			list = append(list, *NewWatchEventForNamespace(ni))
		}
		if typ != l.m.mm.NamespaceType() {
			for _, id := range ni.list(typ) {
				e := l.m._GetElement(id)
				if e != nil {
					list = append(list, *NewWatchEvent(e))
				}
			}
		}
	}
	return list
}
