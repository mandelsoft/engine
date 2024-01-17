package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/default"
	"github.com/mandelsoft/engine/pkg/metamodels/demo"
)

func init() {
	common.MustRegisterType[_default.Namespace](scheme)
}

var scheme = common.NewScheme()

func NewModelSpecification(name string) model.ModelSpecification {
	return model.NewModelSpecification(name, demo.MetaModelSpecification(), scheme)
}
