package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
)

func NewTypeScheme[W wrapper.Object[S], S database.Object](db database.SchemeTypes[S]) database.TypeScheme[W] {
	return wrapper.NewTypeScheme[W, S](db)
}

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
// database objects of interface S.
func NewObjectbase[W Object[S], S database.Object](db database.Database[S], types runtime.SchemeTypes[W]) (objectbase.Objectbase, error) {
	odb, err := wrapper.NewDatabase[objectbase.Object, W, S](db, types, &IdMapping[S]{})
	if err != nil {
		return nil, err
	}
	return objectbase.NewObjectbase(odb), nil
}
