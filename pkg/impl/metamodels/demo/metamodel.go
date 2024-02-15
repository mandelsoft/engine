package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

func init() {
	wrapped.MustRegisterType[support.Namespace](scheme)
}

var scheme = wrapped.NewTypeScheme[support.Object, db2.DBObject](db.Scheme)

func NewModelSpecification(name string, dbspec database.Specification[db2.DBObject]) model.ModelSpecification {
	obspec := wrapped.NewSpecification[support.Object, db2.DBObject](scheme, db.Scheme, dbspec)
	return model.NewModelSpecification(name, mymetamodel.MetaModelSpecification(), obspec)
}
