package events

import (
	"reflect"
	"strings"
	"sync"

	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/logging"
)

var log = logging.DefaultContext().Logger(logging.NewRealm("engine"))

type Id interface {
	GetType() string
	GetNamespace() string
}

type ObjectLister[I Id] interface {
	ListObjectIds(typ string, closure bool, ns string, atomic ...func()) ([]I, error)
}

type EventHandler[I Id] interface {
	HandleEvent(I)
}

type HandlerRegistration[I Id] interface {
	RegisterHandler(h EventHandler[I], current bool, kind string, closure bool, ns string) utils.Sync
	UnregisterHandler(h EventHandler[I], kind string, closure bool, ns string)
}

type HandlerRegistrationTest[I Id] interface {
	HandlerRegistration[I]
	RegisterHandlerSync(t <-chan struct{}, h EventHandler[I], current bool, kind string, closure bool, ns string) utils.Sync
}

type HandlerRegistry[I Id] interface {
	HandlerRegistration[I]
	EventHandler[I]

	TriggerEvent(I)
}

type eventhandlers[I Id] []*wrapper[I]
type namespaces[I Id] map[string]eventhandlers[I]

// KeyFunc provides a pure comparable Id implementation
// usable has map key
// for any element providing the Id interface.

type KeyFunc[I Id] func(id I) I

type registry[I Id] struct {
	lock     sync.Mutex
	key      KeyFunc[I]
	types    map[string]namespaces[I]
	closures map[string]namespaces[I]
	lister   ObjectLister[I]
}

var _ HandlerRegistrationTest[Id] = (*registry[Id])(nil)

func NewHandlerRegistry[I Id](l ObjectLister[I], k ...KeyFunc[I]) HandlerRegistry[I] {
	return &registry[I]{
		key:      general.OptionalDefaulted[KeyFunc[I]](func(id I) I { return id }, k...),
		types:    map[string]namespaces[I]{},
		closures: map[string]namespaces[I]{},
		lister:   l,
	}
}

func (r *registry[I]) HandleEvent(id I) {
	r.TriggerEvent(id)
}

func (r *registry[I]) RegisterHandler(h EventHandler[I], current bool, kind string, closure bool, ns string) utils.Sync {
	s, d := utils.NewSyncPoint()
	if current {
		go func() {
			r.registerHandler(nil, h, current, kind, closure, ns)
			d.Done()
		}()
	} else {
		r.registerHandler(nil, h, current, kind, closure, ns)
		d.Done()
	}
	return s
}

func (r *registry[I]) RegisterHandlerSync(t <-chan struct{}, h EventHandler[I], current bool, kind string, closure bool, ns string) utils.Sync {
	s, d := utils.NewSyncPoint()
	if current {
		go func() {
			r.registerHandler(t, h, current, kind, closure, ns)
			d.Done()
		}()
	} else {
		r.registerHandler(t, h, current, kind, closure, ns)
		d.Done()
	}
	return s
}

func index[I Id](list []*wrapper[I], h EventHandler[I]) int {
	for i, e := range list {
		if e.handler == h {
			return i
		}
	}
	return -1

}

func (r *registry[I]) getReg(closure bool) map[string]namespaces[I] {
	if closure {
		return r.closures
	}
	return r.types
}

func (r *registry[I]) registerHandler(t <-chan struct{}, h EventHandler[I], current bool, kind string, closure bool, ns string) {
	if ns == "" {
		ns = "/"
	}

	r.lock.Lock()

	m := r.getReg(closure)
	nsmap := assure(m, kind)
	handlers := assure(nsmap, ns)
	if index[I](handlers, h) < 0 {
		w := newHandler(h)
		atomic := func() {
			r.lock.Lock()
			nsmap := assure(m, kind)
			handlers := assure(nsmap, ns)
			if index(handlers, h) < 0 {
				nsmap[ns] = append(handlers, w)
			}
			r.lock.Unlock()
		}
		r.lock.Unlock()
		var list []I
		if current {
			list, _ = r.lister.ListObjectIds(kind, closure, ns, atomic)
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

func (r *registry[I]) UnregisterHandler(h EventHandler[I], kind string, closure bool, ns string) {
	if ns == "" {
		ns = "/"
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	m := r.getReg(closure)
	nsmap := m[kind]
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
			delete(m, kind)
		}
	}
}

func (r *registry[I]) getHandlers(id I) []*wrapper[I] {
	r.lock.Lock()
	defer r.lock.Unlock()

	ns := id.GetNamespace()
	if ns == "" {
		ns = "/"
	}
	handlers := r.getHandlersFrom(r.types, id.GetType(), ns)
	for {
		handlers = append(handlers, r.getHandlersFrom(r.closures, id.GetType(), ns)...)
		if ns == "/" {
			break
		}
		i := strings.LastIndex(ns, "/")
		if i < 0 {
			ns = "/"
		} else {
			ns = ns[:i]
		}
	}
	return handlers
}

func (r *registry[I]) getHandlersFrom(reg map[string]namespaces[I], typ string, ns string) []*wrapper[I] {
	var handlers []*wrapper[I]

	nsmap := reg[""]
	if len(nsmap) != 0 {
		handlers = append(handlers, nsmap[ns]...)
	}

	nsmap = reg[typ]
	if len(nsmap) == 0 {
		return handlers
	}
	return append(handlers, nsmap[ns]...)
}

func (r *registry[I]) TriggerEvent(id I) {
	id = r.key(id)
	for _, h := range r.getHandlers(id) {
		log.Trace("trigger event for {{id}}", "id", id)
		h.HandleEvent(id)
	}
}

func assure[T any, K comparable](m map[K]T, k K) T {
	e, ok := m[k]
	if !ok {
		var v reflect.Value
		t := generics.TypeOf[T]()
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
type wrapper[I Id] struct {
	lock    sync.Mutex
	rampup  bool
	queue   []I
	handler EventHandler[I]
}

var _ EventHandler[Id] = (*wrapper[Id])(nil)

func newHandler[I Id](h EventHandler[I]) *wrapper[I] {
	return &wrapper[I]{
		handler: h,
		rampup:  true,
	}
}

func (w *wrapper[I]) handleEvent(id I) {
	w.handler.HandleEvent(id)
}

func (w *wrapper[I]) Rampup(ids []I) {
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

func (w *wrapper[I]) HandleEvent(id I) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.rampup {
		w.queue = append(w.queue, id)
	} else {
		w.handleEvent(id)
	}
}
