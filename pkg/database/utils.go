package database

import (
	"github.com/mandelsoft/engine/pkg/runtime"
)

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s Scheme) {
	runtime.Register[T, P](s, runtime.TypeOf[T]().Name())
}
