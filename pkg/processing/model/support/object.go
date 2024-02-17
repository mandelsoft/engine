package support

import (
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

type _DBObject = db.DBObject

type Object interface {
	objectbase.Object
	wrapped.Object[db.DBObject]
}

type Wrapper struct {
	_DBObject
}

var _ objectbase.Object = (*Wrapper)(nil)
var _ wrapper.Object[db.DBObject] = (*Wrapper)(nil)

func (w *Wrapper) SetBase(o db.DBObject) {
	w._DBObject = o
}

func (w *Wrapper) GetBase() db.DBObject {
	return w._DBObject
}

func (n *Wrapper) AddFinalizer(ob objectbase.Objectbase, f string) (bool, error) {

	mod := func(o db.DBObject) (bool, bool) {
		b := o.AddFinalizer(f)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *Wrapper) RemoveFinalizer(ob objectbase.Objectbase, f string) (bool, error) {

	mod := func(o db.DBObject) (bool, bool) {
		b := o.RemoveFinalizer(f)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}
