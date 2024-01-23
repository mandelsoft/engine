package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	common2 "github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type ObjectId = common2.ObjectId

type Object = common2.Object

type EventHandler = database.EventHandler
type Scheme = common2.Scheme

type Encoding = common2.Encoding
type Objectbase = common2.Objectbase
type SchemeTypes = common2.SchemeTypes
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
