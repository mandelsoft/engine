package model

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Model interface {
	Objectbase() objectbase.Objectbase
	MetaModel() metamodel.MetaModel
	SchemeTypes() objectbase.SchemeTypes
}

type model struct {
	ob    objectbase.Objectbase
	mm    metamodel.MetaModel
	types objectbase.SchemeTypes
}

var _ Model = (*model)(nil)

func NewModel(spec ModelSpecification) (Model, error) {
	err := spec.Validate()
	if err != nil {
		return nil, err
	}
	if spec.MetaModel.NamespaceType == "" {
		return nil, fmt.Errorf("no namespace type specified")
	}
	mm, err := metamodel.NewMetaModel(spec.Name, spec.MetaModel)
	if err != nil {
		return nil, err
	}

	ob, err := spec.Objectbase.CreateObjectbase()
	if err != nil {
		return nil, err
	}
	return &model{ob, mm, spec.Objectbase.SchemeTypes()}, nil
}

func (m *model) SchemeTypes() objectbase.SchemeTypes {
	return m.types
}

func (m *model) Objectbase() objectbase.Objectbase {
	return m.ob
}

func (m *model) MetaModel() metamodel.MetaModel {
	return m.mm
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s Scheme) {
	var i any = s
	runtime.Register[T, P](i.(runtime.Scheme[Object]), utils.TypeOf[T]().Name()) // Goland
}
