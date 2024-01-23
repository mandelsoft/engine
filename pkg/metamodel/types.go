package metamodel

import (
	"slices"
	"strings"

	common2 "github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/utils"
)

const DEFAULT_PHASE = common2.Phase("PhaseUpdating")

type Encoding = common2.Encoding
type Phase = common2.Phase
type ElementId = common2.ElementId
type ObjectId = common2.ObjectId
type TypeId = common2.TypeId

///////////////////////////////////////////////////////////////////////////////

type ElementType interface {
	Id() TypeId

	Dependencies() []ElementType

	addDependency(d ElementType)
}

type elementType struct {
	id           TypeId
	dependencies []ElementType
}

var _ ElementType = (*elementType)(nil)

func newElementType(objtype string, phase common2.Phase) *elementType {
	return &elementType{
		id: common2.NewTypeId(objtype, phase),
	}
}

func (e *elementType) Id() TypeId {
	return e.id
}

func (e *elementType) addDependency(d ElementType) {
	if !slices.Contains(e.dependencies, d) {
		e.dependencies = append(e.dependencies, d)
		slices.SortFunc(e.dependencies, CompareElementType)
	}
}

func (e *elementType) Dependencies() []ElementType {
	return slices.Clone(e.dependencies)
}

func CompareElementType(a, b ElementType) int {
	return strings.Compare(a.Id().String(), b.Id().String())
}

////////////////////////////////////////////////////////////////////////////////

type ExternalObjectType interface {
	Name() string
	Trigger() ElementType
}

type externalObjectType struct {
	name    string
	trigger ElementType
}

var _ ExternalObjectType = (*externalObjectType)(nil)

func newExternalObjectType(name string, trigger ElementType) *externalObjectType {
	return &externalObjectType{
		name:    name,
		trigger: trigger,
	}
}

func (o *externalObjectType) Name() string {
	return o.name
}

func (o *externalObjectType) Trigger() ElementType {
	return o.trigger
}

////////////////////////////////////////////////////////////////////////////////

type InternalObjectType interface {
	Name() string

	Phases() []common2.Phase
	Element(common2.Phase) ElementType
}

type internalObjectType struct {
	name   string
	phases map[common2.Phase]ElementType
}

var _ InternalObjectType = (*internalObjectType)(nil)

func newInternalObjectType(name string, phases map[common2.Phase]ElementType) *internalObjectType {
	return &internalObjectType{
		name:   name,
		phases: phases,
	}
}

func (o *internalObjectType) Name() string {
	return o.name
}

func (o *internalObjectType) Phases() []common2.Phase {
	return utils.OrderedMapKeys(o.phases)
}

func (o *internalObjectType) Element(phase common2.Phase) ElementType {
	return o.phases[phase]
}
