package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	objectbase2 "github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Specification[O Object[DB], DB database.Object] struct {
	ModelTypes runtime.SchemeTypes[O]
	Encoding   database.Encoding[DB]
	Database   database.Specification[DB]
}

var _ objectbase2.Specification = (*Specification[Object[database.Object], database.Object])(nil)

func NewSpecification[O Object[DB], DB database.Object](types runtime.SchemeTypes[O], dbencoding database.Encoding[DB], dbspec database.Specification[DB]) objectbase2.Specification {
	return &Specification[O, DB]{
		ModelTypes: types,
		Encoding:   dbencoding,
		Database:   dbspec,
	}
}

func (s *Specification[O, DB]) SchemeTypes() objectbase2.SchemeTypes {
	r, err := runtime.ConvertTypes[objectbase2.Object](s.ModelTypes)
	if err != nil {
		panic(err)
	}
	return r.(objectbase2.SchemeTypes) // GoLand
}

func (s *Specification[O, DB]) CreateObjectbase() (objectbase2.Objectbase, error) {
	db, err := s.Database.Create(s.Encoding)
	if err != nil {
		return nil, err
	}
	return NewObjectbase[O, DB](db, s.ModelTypes)
}
