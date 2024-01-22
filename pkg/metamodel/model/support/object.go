package support

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type DBObject interface {
	database.Object
	database.GenerationAccess
}

type _DBObject = DBObject

type Object interface {
	objectbase.Object
	wrapped.Object[DBObject]
}

type Wrapper struct {
	_DBObject
}

var _ model.Object = (*Wrapper)(nil)
var _ wrapper.Object[DBObject] = (*Wrapper)(nil)

func (w *Wrapper) SetBase(o DBObject) {
	w._DBObject = o
}

func (w *Wrapper) GetBase() DBObject {
	return w._DBObject
}
