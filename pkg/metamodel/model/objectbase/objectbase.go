package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type ObjectId = common.ObjectId

type Object = common.Object

type EventHandler = database.EventHandler
type Scheme = common.Scheme

type Encoding = common.Encoding
type Objectbase = common.Objectbase
type SchemeTypes = common.SchemeTypes
type Initializer = runtime.Initializer[Object]

func NewScheme() Scheme {
	return database.NewScheme[Object]()
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s database.TypeScheme[Object]) { // Goland: should be Scheme
	database.MustRegisterType[T, Object, P](s)
}

func GetDatabase[O database.Object](ob Objectbase) database.Database[O] {
	if w, ok := ob.(wrapper.Wrapped[O]); ok {
		return w.GetDatabase()
	}
	return nil
}
