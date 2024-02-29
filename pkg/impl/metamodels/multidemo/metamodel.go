package multidemo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	wrapped2 "github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

func init() {
	wrapped2.MustRegisterType[support.Namespace](scheme)
}

var scheme = wrapped2.NewTypeScheme[support.Object, db2.Object](db.Scheme)

func NewModelSpecification(name string, dbspec database.Specification[db2.Object]) model.ModelSpecification {
	obspec := wrapped2.NewSpecification[support.Object, db2.Object](scheme, db.Scheme, dbspec)
	return model.NewModelSpecification(name, mymetamodel.MetaModelSpecification(), obspec)
}
