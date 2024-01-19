package model

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
)

type InternalObject = common.InternalObject
type ExternalObject = common.ExternalObject

type Model interface {
	Objectbase() objectbase.Objectbase
	MetaModel() metamodel.MetaModel
}

type model struct {
	db objectbase.Objectbase
	mm metamodel.MetaModel
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

	return &model{db, mm}, nil
}

func (m *model) Objectbase() objectbase.Objectbase {
	return m.db
}

func (m *model) MetaModel() metamodel.MetaModel {
	return m.mm
}
