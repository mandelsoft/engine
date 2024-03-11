package database

import (
	"strings"

	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, O Object, P pointer[T]](s TypeScheme[O]) {
	runtime.Register[T, P, O](s, utils.TypeOf[T]().Name())
}

////////////////////////////////////////////////////////////////////////////////

func SetObjectName[O Object](ns string, n string) runtime.Initializer[O] {
	return func(o O) {
		o.SetName(n)
		o.SetNamespace(ns)
	}
}

func CompareObjectId(a, b ObjectId) int {
	switch {
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		d := strings.Compare(a.GetNamespace(), b.GetNamespace())
		if d == 0 {
			d = strings.Compare(a.GetName(), b.GetName())
		}
		if d == 0 {
			d = strings.Compare(a.GetType(), b.GetType())
		}
		return d
	}
}

func CompareObject[O ObjectId](a, b O) int {
	return CompareObjectId(a, b)
}

////////////////////////////////////////////////////////////////////////////////

func MatchNamespace(closure bool, ns string, cand string) bool {
	if ns == "/" {
		ns = ""
	}
	if cand == ns {
		return true
	}
	if !closure {
		return false
	}
	if ns != "" {
		return strings.HasPrefix(cand, ns+"/")
	}
	return true
}
