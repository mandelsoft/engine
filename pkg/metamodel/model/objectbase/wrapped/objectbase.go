package hashmapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type IdMapping[S database.Object] struct{}

var _ wrapper.IdMapping[database.Object] = (*IdMapping[database.Object])(nil)

func (m *IdMapping[S]) Namespace(ns string) string {
	return ns
}

func (m *IdMapping[S]) Inbound(id wrapper.ObjectId) wrapper.ObjectId {
	return id
}

func (m *IdMapping[S]) Outbound(id wrapper.ObjectId) wrapper.ObjectId {
	return id
}

func (m *IdMapping[S]) OutboundObject(o S) wrapper.ObjectId {
	return o
}

type Object[S database.Object] interface {
	objectbase.Object
	wrapper.Object[S]
}

// NewObjectbase provides a new object base with functional wrappers (W) for
// database object of interface S.
func NewObjectbase[W Object[S], S database.Object](db database.Database[S], types runtime.SchemeTypes[W]) (objectbase.Objectbase, error) {
	return wrapper.NewDatabase[objectbase.Object, W, S](db, types, &IdMapping[S]{})
}
