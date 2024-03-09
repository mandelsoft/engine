package model

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
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

func MustRegisterType[T any, P pointer[T]](s objectbase.Scheme) {
	var i any = s
	runtime.Register[T, P](i.(runtime.TypeScheme[Object]), utils.TypeOf[T]().Name()) // Goland
}

////////////////////////////////////////////////////////////////////////////////

type rootNamespace struct {
	finalizable database.FinalizedMeta
	kind        string
}

func NewRootNamespace(typ string) NamespaceObject {
	return &rootNamespace{kind: typ}
}

func (r *rootNamespace) HasFinalizer(f string) bool {
	// TODO implement me
	panic("implement me")
}

var _ NamespaceObject = (*rootNamespace)(nil)

func (r *rootNamespace) GetNamespace() string {
	return ""
}

func (r *rootNamespace) GetName() string {
	return ""
}

func (r *rootNamespace) GetType() string {
	return r.kind
}

func (r *rootNamespace) SetType(s string) {
	r.kind = s
}

func (r *rootNamespace) SetName(s string) {
	panic("not supported")
}

func (r *rootNamespace) SetNamespace(s string) {
	panic("not supported")
}

func (r *rootNamespace) GetGeneration() int64 {
	return 0
}

func (r *rootNamespace) SetGeneration(i int64) {
	panic("not supported")
}

func (r *rootNamespace) IsDeleting() bool {
	return false
}

func (r *rootNamespace) GetFinalizers() []string {
	return r.finalizable.GetFinalizers()
}

func (r *rootNamespace) AddFinalizer(ob internal.Objectbase, f string) (bool, error) {
	return r.finalizable.AddFinalizer(f), nil
}

func (r *rootNamespace) RemoveFinalizer(ob internal.Objectbase, f string) (bool, error) {
	return r.finalizable.RemoveFinalizer(f), nil
}

func (r *rootNamespace) GetNamespaceName() string {
	return ""
}

func (r rootNamespace) GetLock() mmids.RunId {
	return ""
}

func (r rootNamespace) ClearLock(ob internal.Objectbase, id mmids.RunId) (bool, error) {
	return false, nil
}

func (r rootNamespace) TryLock(db internal.Objectbase, id mmids.RunId) (bool, error) {
	return false, nil
}
