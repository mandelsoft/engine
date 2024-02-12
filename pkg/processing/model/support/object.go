package support

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
)

type DBObject interface {
	database.Object
	database.GenerationAccess
	database.StatusSource
}

type _DBObject = DBObject

type Object interface {
	objectbase.Object
	wrapped.Object[DBObject]
}

type Wrapper struct {
	_DBObject
}

var _ objectbase.Object = (*Wrapper)(nil)
var _ wrapper.Object[DBObject] = (*Wrapper)(nil)

func (w *Wrapper) SetBase(o DBObject) {
	w._DBObject = o
}

func (w *Wrapper) GetBase() DBObject {
	return w._DBObject
}
