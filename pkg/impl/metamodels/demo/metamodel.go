package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	wrapped2 "github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/metamodels/demo"
)

func init() {
	wrapped2.MustRegisterType[support.Namespace](scheme)
}

var scheme = wrapped2.NewTypeScheme[support.Object, support.DBObject](db.Scheme)

func NewModelSpecification(name string, dbspec database.Specification[support.DBObject]) model.ModelSpecification {
	obspec := wrapped2.NewSpecification[support.Object, support.DBObject](scheme, db.Scheme, dbspec)
	return model.NewModelSpecification(name, demo.MetaModelSpecification(), obspec)
}
