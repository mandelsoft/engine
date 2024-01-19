package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/default"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodels/demo"
)

func init() {
	objectbase.MustRegisterType[_default.Namespace](scheme)
}

var scheme = objectbase.NewScheme()

func NewModelSpecification(name string, dbspec database.Specification[objectbase.Object]) model.ModelSpecification {
	return model.NewModelSpecification(name, demo.MetaModelSpecification(), &specification{dbspec})
}

type specification struct {
	dbspec database.Specification[objectbase.Object]
}

var _ objectbase.Specification = (*specification)(nil)

func (s *specification) SchemeTypes() objectbase.SchemeTypes {
	return scheme.(objectbase.SchemeTypes) // Goland requires type cast
}

func (s *specification) CreateObjectbase() (objectbase.Objectbase, error) {
	return s.dbspec.Create(scheme)
}
