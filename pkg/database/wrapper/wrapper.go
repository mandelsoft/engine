package wrapper

import (
	"context"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Object[S database.Object] interface {
	database.Object

	SetBase(S)
	GetBase() S
}

type ObjectId = database.ObjectId

type Wrapped[O database.Object] interface {
	GetDatabase() database.Database[O]
}

type Database[O database.Object, W Object[S], S database.Object] interface {
	database.Database[O]
	Wrapped[S]
	WrapObject(s S) (O, error)
}

type IdMapping[S database.Object] interface {
	// Namespace maps an outer namespace to an inner one.
	// The inner one may contain objects from multiple outer namespaces.
	Namespace(string) string

	// Inbound maps an outer object id to an inner one.
	Inbound(ObjectId) ObjectId
	// Outbound maps an inner object id to an outer one.
	Outbound(ObjectId) ObjectId
	OutboundObject(o S) ObjectId
}

type wrappingDatabase[O database.Object, W Object[S], S database.Object] struct {
	db        database.Database[S]
	types     runtime.SchemeTypes[O]
	create    runtime.SchemeTypes[W]
	idmapping IdMapping[S]
	events    database.HandlerRegistry
}

var _ Database[database.Object, Object[database.Object], database.Object] = (*wrappingDatabase[database.Object, Object[database.Object], database.Object])(nil)
var _ database.HandlerRegistrationTest = (*wrappingDatabase[database.Object, Object[database.Object], database.Object])(nil)

// NewDatabase provides a database.Database[O] introduction functional
// wrappers (W) on top of technical objects (S) persisted in a database.
// W must implement O, which cannot be expressed in Go.
func NewDatabase[O database.Object, W Object[S], S database.Object](db database.Database[S], create runtime.SchemeTypes[W], idmap IdMapping[S]) (Database[O, W, S], error) {
	types, err := runtime.ConvertTypes[O, W](create)
	if err != nil {
		return nil, err
	}
	r := &wrappingDatabase[O, W, S]{
		db:        db,
		types:     types,
		create:    create,
		idmapping: idmap,
	}
	events := database.NewHandlerRegistry(r)
	r.events = events
	db.RegisterHandler(&handler[O, W, S]{r}, false, "", true, "/").Wait(context.Background())
	return r, nil
}

type handler[O database.Object, W Object[S], S database.Object] struct {
	db *wrappingDatabase[O, W, S]
}

func (h *handler[O, W, S]) HandleEvent(sid database.ObjectId) {
	id := h.db.idmapping.Outbound(sid)
	if id.GetName() != "" {
		h.db.events.TriggerEvent(id)
	}
}

func (w *wrappingDatabase[O, W, S]) GetDatabase() database.Database[S] {
	return w.db
}

func (w *wrappingDatabase[O, W, S]) SchemeTypes() database.SchemeTypes[O] {
	return w.types
}

func (w *wrappingDatabase[O, W, S]) RegisterHandler(h database.EventHandler, current bool, kind string, closure bool, ns string) utils.Sync {
	return w.events.RegisterHandler(h, current, kind, closure, ns)
}

func (w *wrappingDatabase[O, W, S]) RegisterHandlerSync(t <-chan struct{}, h database.EventHandler, current bool, kind string, closure bool, ns string) utils.Sync {
	return w.events.(database.HandlerRegistrationTest).RegisterHandlerSync(t, h, current, kind, closure, ns)
}

func (w *wrappingDatabase[O, W, S]) UnregisterHandler(h database.EventHandler, kind string, closure bool, ns string) {
	w.events.UnregisterHandler(h, kind, closure, ns)
}

func (w *wrappingDatabase[O, W, S]) ListObjectIds(typ string, closure bool, ns string, atomic ...func()) ([]database.ObjectId, error) {
	basens := w.idmapping.Namespace(ns)
	list, err := w.db.ListObjectIds(typ, closure, basens, atomic...)
	if err != nil {
		return nil, err
	}
	r := []database.ObjectId{}
	for _, sid := range list {
		id := w.idmapping.Outbound(sid)
		if !database.MatchNamespace(closure, ns, id.GetNamespace()) {
			continue
		}
		r = append(r, id)
	}
	return r, nil
}

func (w *wrappingDatabase[O, W, S]) ListObjects(typ string, closure bool, ns string) ([]O, error) {
	basens := w.idmapping.Namespace(ns)
	list, err := w.db.ListObjects(typ, closure, basens)
	if err != nil {
		return nil, err
	}
	r := []O{}
	for _, b := range list {
		id := w.idmapping.OutboundObject(b)
		if !database.MatchNamespace(closure, ns, id.GetNamespace()) {
			continue
		}
		e, err := w.WrapObject(b)
		if err != nil {
			return nil, err
		}
		r = append(r, e)
	}
	return r, nil
}

func (w *wrappingDatabase[O, W, S]) GetObject(id database.ObjectId) (O, error) {
	var _nil O

	sid := w.idmapping.Inbound(id)

	o, err := w.create.CreateObject(id.GetType(), database.SetObjectName[W](id.GetNamespace(), id.GetName()))
	if err != nil {
		return _nil, err
	}

	b, err := w.db.GetObject(sid)
	if err != nil {
		return _nil, err
	}
	o.SetBase(b)
	return utils.Cast[O](o), nil

}

func (w *wrappingDatabase[O, W, S]) WrapObject(s S) (O, error) {
	var _nil O

	id := w.idmapping.OutboundObject(s)
	o, err := w.create.CreateObject(id.GetType(), database.SetObjectName[W](id.GetNamespace(), id.GetName()))
	if err != nil {
		return _nil, err
	}
	o.SetBase(s)
	return utils.Cast[O](o), nil
}

func (w *wrappingDatabase[O, W, S]) SetObject(o O) error {
	i, ok := utils.TryCast[W](o)
	if !ok {
		return fmt.Errorf("invalid Go type %T", o)
	}
	return w.db.SetObject(i.GetBase())
}

func (w *wrappingDatabase[O, W, S]) DeleteObject(id database.ObjectId) (bool, error) {
	sid := w.idmapping.Inbound(id)
	return w.db.DeleteObject(sid)
}
