package wrapper

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type TypeScheme[W Object[S], S database.Object] struct {
	database.TypeScheme[W]
	base database.SchemeTypes[S]
}

func NewTypeScheme[W Object[S], S database.Object](base database.SchemeTypes[S]) database.TypeScheme[W] {
	return &TypeScheme[W, S]{
		TypeScheme: runtime.NewTypeScheme[W](),
		base:       base,
	}
}

func (s *TypeScheme[W, S]) CreateObject(typ string, init ...runtime.Initializer[W]) (W, error) {
	var _nil W

	b, err := s.base.CreateObject(typ)
	if err != nil {
		return _nil, err
	}
	return s.TypeScheme.CreateObject(typ, append([]runtime.Initializer[W]{func(o W) {
		o.SetBase(b)
	}}, init...)...)
}
