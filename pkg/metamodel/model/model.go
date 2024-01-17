package model

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Model interface {
	Database() database.Database
	MetaModel() metamodel.MetaModel
}

type model struct {
	db database.Database
	mm metamodel.MetaModel
}

var _ Model = (*model)(nil)

func NewModel(dbspec database.Specification, inst ModelSpecification) (Model, error) {
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

	dbenc, err := runtime.ConvertEncoding[database.Object](inst.Scheme)
	if err != nil {
		return nil, err
	}

	db, err := dbspec.Create(dbenc)
	if err != nil {
		return nil, err
	}
	return &model{db, mm}, nil
}

func (m *model) Database() database.Database {
	return m.db
}

func (m *model) MetaModel() metamodel.MetaModel {
	return m.mm
}
