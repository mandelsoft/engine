package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/default"
	"github.com/mandelsoft/engine/pkg/metamodel/demo"
)

func init() {
	metamodel.MustRegisterType[_default.Namespace](scheme)
}

var scheme = metamodel.NewScheme()

type MetaModel struct {
	demo.MetaModelBase
}

func NewMetaMode() metamodel.MetaModel {
	return &MetaModel{}
}

var _ demo.MetaModel = (*MetaModel)(nil)

func (m *MetaModel) GetEncoding() metamodel.Encoding {
	return scheme
}
