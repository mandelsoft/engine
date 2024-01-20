package database

import (
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, O Object, P pointer[T]](s Scheme[O]) {
	runtime.Register[T, P, O](s, utils.TypeOf[T]().Name())
}

////////////////////////////////////////////////////////////////////////////////

func SetObjectName[O Object](ns string, n string) runtime.Initializer[O] {
	return func(o O) {
		o.SetName(n)
		o.SetNamespace(ns)
	}
}
