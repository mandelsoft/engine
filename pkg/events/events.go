package events

import (
	"reflect"
	"sync"

	"github.com/mandelsoft/engine/pkg/utils"
)

type Id interface {
	GetType() string
	GetNamespace() string
}

type ObjectLister[I Id] interface {
	ListObjectIds(typ string, ns string, atomic ...func()) ([]I, error)
}

type EventHandler[I Id] interface {
	HandleEvent(I)
}

type HandlerRegistration[I Id] interface {
	RegisterHandler(h EventHandler[I], current bool, kind string, nss ...string) utils.Sync
	UnregisterHandler(h EventHandler[I], kind string, nss ...string)
}

type HandlerRegistrationTest[I Id] interface {
	HandlerRegistration[I]
	RegisterHandlerSync(t <-chan struct{}, h EventHandler[I], current bool, kind string, nss ...string) utils.Sync
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
	lock   sync.Mutex
	key    KeyFunc[I]
	types  map[string]namespaces[I]
	lister ObjectLister[I]
}

var _ HandlerRegistrationTest[Id] = (*registry[Id])(nil)

func NewHandlerRegistry[I Id](l ObjectLister[I], k ...KeyFunc[I]) HandlerRegistry[I] {
	return &registry[I]{
		key:    utils.OptionalDefaulted[KeyFunc[I]](func(id I) I { return id }, k...),
		types:  map[string]namespaces[I]{},
		lister: l,
	}
}

func (r *registry[I]) HandleEvent(id I) {
	r.TriggerEvent(id)
}

func (r *registry[I]) RegisterHandler(h EventHandler[I], current bool, kind string, nss ...string) utils.Sync {
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

func (r *registry[I]) RegisterHandlerSync(t <-chan struct{}, h EventHandler[I], current bool, kind string, nss ...string) utils.Sync {
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

func index[I Id](list []*wrapper[I], h EventHandler[I]) int {
	for i, e := range list {
		if e.handler == h {
			return i
		}
	}
	return -1

}
func (r *registry[I]) registerHandler(t <-chan struct{}, h EventHandler[I], current bool, kind string, nss ...string) {
	if len(nss) == 0 {
		nss = []string{""}
	}

	for _, ns := range nss {
		r.lock.Lock()
		nsmap := assure(r.types, kind)
		handlers := assure(nsmap, ns)
		if index[I](handlers, h) < 0 {
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
			var list []I
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

func (r *registry[I]) UnregisterHandler(h EventHandler[I], kind string, nss ...string) {
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

func (r *registry[I]) getHandlers(id I) []*wrapper[I] {
	r.lock.Lock()
	defer r.lock.Unlock()

	var handlers []*wrapper[I]
	ns := id.GetNamespace()
	if ns == "" {
		ns = "/"
	}
	nsmap := r.types[""]
	if len(nsmap) != 0 {
		handlers = append(handlers, nsmap[ns]...)
		handlers = append(handlers, nsmap[""]...)
	}

	nsmap = r.types[id.GetType()]
	if len(nsmap) == 0 {
		return handlers
	}
	handlers = append(handlers, nsmap[ns]...)
	return append(handlers, nsmap[""]...)
}

func (r *registry[I]) TriggerEvent(id I) {
	id = r.key(id)
	for _, h := range r.getHandlers(id) {
		h.HandleEvent(id)
	}
}

func assure[T any, K comparable](m map[K]T, k K) T {
	e := m[k]
	if reflect.ValueOf(e).IsZero() {
		var v reflect.Value
		t := utils.TypeOf[T]()
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
