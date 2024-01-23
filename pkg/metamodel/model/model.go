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
	db    objectbase.Objectbase
	mm    metamodel.MetaModel
	types objectbase.SchemeTypes
}

var _ Model = (*model)(nil)

func NewModel(db objectbase.Objectbase, inst ModelSpecification) (Model, error) {
	err := inst.Validate()
	if err != nil {
		return nil, err
	}
	if inst.MetaModel.NamespaceType == "" {
		return nil, fmt.Errorf("no namespace type specified")
	}
	mm, err := metamodel.NewMetaModel(inst.Name, inst.MetaModel)
	if err != nil {
		return nil, err
	}

	return &model{db, mm, inst.Objectbase.SchemeTypes()}, nil
}

func (m *model) SchemeTypes() objectbase.SchemeTypes {
	return m.types
}

func (m *model) Objectbase() objectbase.Objectbase {
	return m.db
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
