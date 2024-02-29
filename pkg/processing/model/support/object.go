package support

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	wrapped2 "github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
)

type _DBObject = db.Object

type Object interface {
	objectbase.Object
	wrapped2.Object[db.Object]
}

type Wrapper struct {
	_DBObject
}

var _ objectbase.Object = (*Wrapper)(nil)
var _ wrapper.Object[db.Object] = (*Wrapper)(nil)

func (w *Wrapper) SetBase(o db.Object) {
	w._DBObject = o
}

func (w *Wrapper) GetBase() db.Object {
	return w._DBObject
}

func (n *Wrapper) AddFinalizer(ob objectbase.Objectbase, f string) (bool, error) {
	mod := func(o db.Object) (bool, bool) {
		b := o.AddFinalizer(f)
		return b, b
	}
	b, err := wrapped2.Modify(ob, n, mod)
	if b {
		database.Log.Debug("adding finalizer {{finalizer}} for {{oid}}: {{effective}}", "oid", database.NewObjectIdFor(n), "finalizer", f, "effective", n.GetFinalizers())
	}
	return b, err
}

func (n *Wrapper) RemoveFinalizer(ob objectbase.Objectbase, f string) (bool, error) {
	mod := func(o db.Object) (bool, bool) {
		b := o.RemoveFinalizer(f)
		return b, b
	}
	b, err := wrapped2.Modify(ob, n, mod)
	if b {
		database.Log.Debug("removing finalizer {{finalizer}} for {{oid}}: {{effective}}", "oid", database.NewObjectIdFor(n), "finalizer", f, "effective", n.GetFinalizers())
	}
	return b, err
}
