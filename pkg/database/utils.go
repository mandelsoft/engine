package database

import (
	"github.com/mandelsoft/engine/pkg/runtime"
)

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, O Object, P pointer[T]](s Scheme[O]) {
	runtime.Register[T, P, O](s, runtime.TypeOf[T]().Name())
}
