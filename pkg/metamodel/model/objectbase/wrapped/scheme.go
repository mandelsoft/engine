package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type pointer[P any, S database.Object] interface {
	Object[S]
	*P
}

func MustRegisterType[T any, P pointer[T, S], W wrapper.Object[S], S database.Object](s database.TypeScheme[W]) { // Goland: should be Scheme
	database.MustRegisterType[T, W, P](s)
}
