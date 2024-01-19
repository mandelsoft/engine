package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type ObjectId = common.ObjectId

type Object = common.Object

type EventHandler = database.EventHandler
type Scheme = common.Scheme

type Encoding = common.Encoding
type Objectbase = common.Objectbase

type SchemeTypes = database.SchemeTypes[Object]

func NewScheme() Scheme {
	var s any = database.NewScheme[Object]() // Goland
	return s.(Scheme)
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s database.Scheme[Object]) { // Goland: should be Scheme
	database.MustRegisterType[T, Object, P](s)
}
