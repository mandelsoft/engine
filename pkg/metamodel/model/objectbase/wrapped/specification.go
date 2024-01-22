package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Specification[O Object[DB], DB database.Object] struct {
	ModelTypes runtime.SchemeTypes[O]
	Encoding   database.Encoding[DB]
	Database   database.Specification[DB]
}

var _ objectbase.Specification = (*Specification[Object[database.Object], database.Object])(nil)

func NewSpecification[O Object[DB], DB database.Object](types runtime.SchemeTypes[O], dbencoding database.Encoding[DB], dbspec database.Specification[DB]) objectbase.Specification {
	return &Specification[O, DB]{
		ModelTypes: types,
		Encoding:   dbencoding,
		Database:   dbspec,
	}
}

func (s *Specification[O, DB]) SchemeTypes() objectbase.SchemeTypes {
	r, err := runtime.ConvertTypes[objectbase.Object](s.ModelTypes)
	if err != nil {
		panic(err)
	}
	return r.(objectbase.SchemeTypes) // GoLand
}

func (s *Specification[O, DB]) CreateObjectbase() (objectbase.Objectbase, error) {
	db, err := s.Database.Create(s.Encoding)
	if err != nil {
		return nil, err
	}
	return NewObjectbase[O, DB](db, s.ModelTypes)
}
