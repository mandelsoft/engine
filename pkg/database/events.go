package database

import (
	"reflect"
	"sync"

	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type HandlerRegistration interface {
	RegisterHandler(h EventHandler, current bool, kind string, nss ...string) utils.Sync
	UnregisterHandler(h EventHandler, kind string, nss ...string)
}

type HandlerRegistrationTest interface {
	HandlerRegistration
	RegisterHandlerSync(t <-chan struct{}, h EventHandler, current bool, kind string, nss ...string) utils.Sync
}

type HandlerRegistry interface {
	HandlerRegistration
	EventHandler

	TriggerEvent(ObjectId)
}

type eventhandlers []*wrapper
type namespaces map[string]eventhandlers

type registry struct {
	lock   sync.Mutex
	types  map[string]namespaces
	lister ObjectLister
}

var _ HandlerRegistrationTest = (*registry)(nil)

func NewHandlerRegistry(l ObjectLister) HandlerRegistry {
	return &registry{
		types:  map[string]namespaces{},
		lister: l,
	}
}

func (r *registry) HandleEvent(id ObjectId) {
	r.TriggerEvent(id)
}

func (r *registry) RegisterHandler(h EventHandler, current bool, kind string, nss ...string) utils.Sync {
	s, d := utils.NewSyncPoint()
	if current {
		go func() {
			r.registerHandler(nil, h, current, kind, nss...)
			d.Done()
		}()
	} else {
		r.registerHandler(nil, h, current, kind, nss...)
		d.Done()
	}
	return s
}

func (r *registry) RegisterHandlerSync(t <-chan struct{}, h EventHandler, current bool, kind string, nss ...string) utils.Sync {
	s, d := utils.NewSyncPoint()
	if current {
		go func() {
			r.registerHandler(t, h, current, kind, nss...)
			d.Done()
		}()
	} else {
		r.registerHandler(t, h, current, kind, nss...)
		d.Done()
	}
	return s
}
func index(list []*wrapper, h EventHandler) int {
	for i, e := range list {
		if e.handler == h {
			return i
		}
	}
	return -1

}
func (r *registry) registerHandler(t <-chan struct{}, h EventHandler, current bool, kind string, nss ...string) {
	if len(nss) == 0 {
		nss = []string{""}
	}

	for _, ns := range nss {
		r.lock.Lock()
		nsmap := assure(r.types, kind)
		handlers := assure(nsmap, ns)
		if index(handlers, h) < 0 {
			w := newHandler(h)
			atomic := func() {
				r.lock.Lock()
				nsmap := assure(r.types, kind)
				handlers := assure(nsmap, ns)
				if index(handlers, h) < 0 {
					nsmap[ns] = append(handlers, w)
				}
				r.lock.Unlock()
			}
			r.lock.Unlock()
			var list []ObjectId
			if current {
				list, _ = r.lister.ListObjectIds(kind, ns, atomic)
			} else {
				atomic()
			}
			if t != nil {
				<-t
			}
			w.Rampup(list)
		} else {
			r.lock.Unlock()
		}
	}
}

func (r *registry) UnregisterHandler(h EventHandler, kind string, nss ...string) {
	if len(nss) == 0 {
		nss = []string{""}
	}
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, ns := range nss {
		nsmap := r.types[kind]
		if nsmap != nil {
			handlers := nsmap[ns]
			if handlers != nil {
				if i := index(handlers, h); i >= 0 {
					handlers = append(handlers[:i], handlers[i+1:]...)
				}
				if len(handlers) > 0 {
					nsmap[ns] = handlers
				} else {
					delete(nsmap, ns)
				}
			}
			if len(nsmap) == 0 {
				delete(r.types, kind)
			}
		}
	}
}

func (r *registry) getHandlers(id ObjectId) []*wrapper {
	r.lock.Lock()
	defer r.lock.Unlock()

	var handlers []*wrapper
	nsmap := r.types[""]
	if len(nsmap) != 0 {
		if id.GetNamespace() != "" {
			handlers = append(handlers, nsmap[id.GetNamespace()]...)
		}
		handlers = append(handlers, nsmap[""]...)
	}

	nsmap = r.types[id.GetType()]
	if len(nsmap) == 0 {
		return handlers
	}
	if id.GetNamespace() != "" {
		handlers = append(handlers, nsmap[id.GetNamespace()]...)
	}
	return append(handlers, nsmap[""]...)
}

func (r *registry) TriggerEvent(id ObjectId) {
	id = NewObjectIdFor(id) // enforce pure id object
	for _, h := range r.getHandlers(id) {
		h.HandleEvent(id)
	}
}

func assure[T any, K comparable](m map[K]T, k K) T {
	e := m[k]
	if reflect.ValueOf(e).IsZero() {
		var v reflect.Value
		t := runtime.TypeOf[T]()
		if t.Kind() == reflect.Map {
			v = reflect.MakeMap(t)
		} else {
			v = reflect.New(t).Elem()
		}
		e = v.Interface().(T)
		m[k] = e
	}
	return e
}

// wrapper handles the rampup of a handler.
// It queues new event until events for actual ids are
// propagated.
type wrapper struct {
	lock    sync.Mutex
	rampup  bool
	queue   []ObjectId
	handler EventHandler
}

var _ EventHandler = (*wrapper)(nil)

func newHandler(h EventHandler) *wrapper {
	return &wrapper{
		handler: h,
		rampup:  true,
	}
}

func (w *wrapper) handleEvent(id ObjectId) {
	w.handler.HandleEvent(id)
}

func (w *wrapper) Rampup(ids []ObjectId) {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, id := range ids {
		w.handleEvent(id)
	}

	for _, id := range w.queue {
		w.handleEvent(id)
	}
	w.rampup = false
	w.queue = nil
}

func (w *wrapper) HandleEvent(id ObjectId) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.rampup {
		w.queue = append(w.queue, id)
	} else {
		w.handleEvent(id)
	}
}
