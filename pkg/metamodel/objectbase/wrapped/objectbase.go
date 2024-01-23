package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	wrapper2 "github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
)

func NewTypeScheme[W wrapper2.Object[S], S database.Object](db database.SchemeTypes[S]) database.TypeScheme[W] {
	return wrapper2.NewTypeScheme[W, S](db)
}

type IdMapping[S database.Object] struct{}

var _ wrapper2.IdMapping[database.Object] = (*IdMapping[database.Object])(nil)

func (m *IdMapping[S]) Namespace(ns string) string {
	return ns
}

func (m *IdMapping[S]) Inbound(id wrapper2.ObjectId) wrapper2.ObjectId {
	return id
}

func (m *IdMapping[S]) Outbound(id wrapper2.ObjectId) wrapper2.ObjectId {
	return id
}

func (m *IdMapping[S]) OutboundObject(o S) wrapper2.ObjectId {
	return o
}

type Object[S database.Object] interface {
	objectbase.Object
	wrapper2.Object[S]
}

// NewObjectbase provides a new object base with functional wrappers (W) for
// database object of interface S.
func NewObjectbase[W Object[S], S database.Object](db database.Database[S], types runtime.SchemeTypes[W]) (objectbase.Objectbase, error) {
	return wrapper2.NewDatabase[objectbase.Object, W, S](db, types, &IdMapping[S]{})
}
