package sub

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	wrapped.MustRegisterType[support.Namespace](scheme)
	wrapped.MustRegisterType[support.UpdateRequest](scheme)
}

var scheme = wrapped.NewTypeScheme[support.Object, db2.Object](db.Scheme)

func NewModelSpecification(name string, dbspec database.Specification[db2.Object]) model.ModelSpecification {
	mmspec := mymetamodel.MetaModelSpecification()
	mmspec.UpdateRequestType = mymetamodel.TYPE_UPDATEREQUEST
	obspec := wrapped.NewSpecification[support.Object, db2.Object](scheme, db.Scheme, dbspec)
	return model.NewModelSpecification(name, mmspec, obspec)
}
