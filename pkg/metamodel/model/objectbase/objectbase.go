package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type ObjectId = common.ObjectId

type Object = common.Object

type EventHandler = database.EventHandler
type Scheme = database.Scheme[Object]
type Encoding = database.Encoding[Object]
type Objectbase = common.Objectbase

type SchemeTypes = database.SchemeTypes[Object]

func NewScheme() Scheme {
	return database.NewScheme[Object]()
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s Scheme) {
	database.MustRegisterType[T, Object, P](s)
}
