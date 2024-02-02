package valopdemo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[support.Namespace](scheme)
}

var scheme = wrapped.NewTypeScheme[support.Object, support.DBObject](db.Scheme)

func NewModelSpecification(name string, dbspec database.Specification[support.DBObject]) model.ModelSpecification {
	obspec := wrapped.NewSpecification[support.Object, support.DBObject](scheme, db.Scheme, dbspec)
	return model.NewModelSpecification(name, mymetamodel.MetaModelSpecification(), obspec)
}