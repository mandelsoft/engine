package objectspace

import (
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
)

var NS = "testspace"

var MetaModel metamodel.MetaModel

func init() {
	MetaModel, _ = mymetamodel.NewMetaModel("demo")
}
