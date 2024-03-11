package main

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/mandelsoft/engine/pkg/processing/model"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/watch"
	"k8s.io/apimachinery/pkg/util/rand"
)

type Trigger interface {
}
type ObjectSpace struct {
	lock sync.RWMutex

	registry elemwatch.Registry
	lister   *ObjectLister
	list     []elemwatch.Id
	objects  map[elemwatch.Id]*elemwatch.Event
}

var _ watch.Registry[elemwatch.Request, elemwatch.Event] = (*ObjectSpace)(nil)

func NewObjectSpace() *ObjectSpace {
	rand.Seed(time.Now().UnixNano())
	s := &ObjectSpace{
		objects: map[elemwatch.Id]*elemwatch.Event{},
	}
	s.lister = NewObjectLister(s)
	s.registry = elemwatch.NewRegistry(s.lister)
	return s
}

func (s *ObjectSpace) RegisterWatchHandler(r elemwatch.Request, h elemwatch.EventHandler) {
	s.registry.RegisterWatchHandler(r, h)
}

func (s *ObjectSpace) UnregisterWatchHandler(r elemwatch.Request, h elemwatch.EventHandler) {
	s.registry.UnregisterWatchHandler(r, h)
}

func (s *ObjectSpace) ChooseRandomObject() *elemwatch.Event {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if len(s.list) == 0 {
		return nil
	}
	return s.Get(Random(s.list))
}

func (s *ObjectSpace) ChooseRandomNamespace() *elemwatch.Event {
	s.lock.RLock()
	defer s.lock.RUnlock()

	list := utils.FilterSlice(s.list, func(id elemwatch.Id) bool { return id.Phase == "" })
	return s.Get(Random(list))
}

func (s *ObjectSpace) IsUsed(id elemwatch.Id) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if id.Phase == "" {
		ns := NamespaceName(id)
		for _, e := range s.objects {
			if e.GetNamespace() == ns {
				return true
			}
		}
	}
	for _, e := range s.objects {
		if slices.Contains(e.Links, id) {
			return true
		}
	}
	return false
}

func (s *ObjectSpace) IsCycle(a, b *elemwatch.Event) bool {
	return slices.Contains(s.GetGraph(b), a)
}

func (s *ObjectSpace) GetGraph(o *elemwatch.Event) []*elemwatch.Event {
	s.lock.RLock()
	defer s.lock.RUnlock()

	graph := []*elemwatch.Event{o}

	for i := 0; i < len(graph); i++ {
		graph = append(graph, s.getGraph(graph[i], graph)...)
	}
	return graph
}

func (s *ObjectSpace) getGraph(c *elemwatch.Event, cur []*elemwatch.Event) []*elemwatch.Event {
	var r []*elemwatch.Event

	for _, e := range s.objects {
		if slices.Contains(e.Links, c.Node) {
			r = append(r, e)
		}
	}
	return r
}

func (s *ObjectSpace) Has(id elemwatch.Id) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.objects[id] != nil
}

func (s *ObjectSpace) Delete(id elemwatch.Id) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	i := slices.Index(s.list, id)
	if i >= 0 {
		s.list = slices.Delete(s.list, i, i+1)
		e := elemwatch.Event{
			Node:   id,
			Status: string(model.STATUS_DELETED),
		}
		s.registry.TriggerEvent(e)
	}
}

func (s *ObjectSpace) Get(id elemwatch.Id) *elemwatch.Event {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.objects[id] == nil {
		return nil
	}
	e := *s.objects[id]
	e.Links = slices.Clone(e.Links)
	return &e
}

func (s *ObjectSpace) Set(evt *elemwatch.Event) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.objects[evt.Node]; !ok {
		s.list = append(s.list, evt.Node)
	}
	e := *evt
	e.Links = slices.Clone(evt.Links)
	s.objects[evt.Node] = &e
	s.registry.TriggerEvent(e)
}

func (s *ObjectSpace) List(typ, ns string, p string) []elemwatch.Id {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var list []elemwatch.Id

	for id, evt := range s.objects {
		if ns != "" && id.GetNamespace() != ns {
			continue
		}
		if typ != "" && id.GetType() != typ {
			continue
		}
		if p != "" && id.Phase != p {
			continue
		}
		list = append(list, evt.Node)
	}
	return list
}

////////////////////////////////////////////////////////////////////////////////

type ObjectLister struct {
	objects *ObjectSpace
}

var _ elemwatch.ObjectLister = (*ObjectLister)(nil)

func NewObjectLister(s *ObjectSpace) *ObjectLister {
	return &ObjectLister{objects: s}
}

func (l *ObjectLister) ListObjectIds(typ string, ns string, atomic ...func()) ([]elemwatch.Event, error) {
	l.objects.lock.RLock()
	defer l.objects.lock.RUnlock()

	var list []elemwatch.Event

	for id, evt := range l.objects.objects {
		if ns != "" && id.GetNamespace() != ns {
			continue
		}
		if typ != "" && id.GetType() != typ {
			continue
		}
		list = append(list, *evt)
	}

	for _, a := range atomic {
		a()
	}
	return list, nil
}

func NamespaceName(id elemwatch.Id) string {
	if id.GetNamespace() == "" {
		return id.Name
	}
	return fmt.Sprintf("%s/%s", id.Namespace, id.Name)
}
