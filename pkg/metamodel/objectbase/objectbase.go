package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/runtime"
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

type objectbase struct {
	database.Database[Object]
}

func NewObjectbase(db database.Database[Object]) Objectbase {
	return &objectbase{db}
}

func (d *objectbase) CreateObject(id ObjectId) (Object, error) {
	return d.SchemeTypes().CreateObject(id.Type(), SetObjectName(id.Namespace(), id.Name()))
}

func GetDatabase[O database.Object](ob Objectbase) database.Database[O] {
	if w, ok := ob.(wrapper.Wrapped[O]); ok {
		return w.GetDatabase()
	}
	return nil
}
