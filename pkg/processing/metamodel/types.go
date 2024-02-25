package metamodel

import (
	"slices"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/utils"
)

const DEFAULT_PHASE = Phase("PhaseUpdating")

///////////////////////////////////////////////////////////////////////////////

type elementType struct {
	id           TypeId
	dependencies []*elementType
	trigger      *string
	states       []string
}

var _ ElementType = (*elementType)(nil)
var _elementType = utils.CastPointer[ElementType, elementType]

func newElementType(objtype string, phase Phase) *elementType {
	return &elementType{
		id: NewTypeId(objtype, phase),
	}
}

func (e *elementType) Id() TypeId {
	return e.id
}

func (e *elementType) addDependency(d *elementType) {
	if !slices.Contains(e.dependencies, d) {
		e.dependencies = append(e.dependencies, d)
		slices.SortFunc(e.dependencies, compareElementType)
	}
}

func (e *elementType) setTrigger(typ string) {
	e.trigger = &typ
}

func (e *elementType) Dependencies() []ElementType {
	return utils.CastPointerSlice[ElementType](e.dependencies)
}

func (e *elementType) HasDependency(name TypeId) bool {
	for _, d := range e.dependencies {
		if d.Id() == name {
			return true
		}
	}
	return false
}

func (e *elementType) TriggeredBy() *string {
	return e.trigger
}

func (e *elementType) ExternalStates() []string {
	return slices.Clone(e.states)
}

func CompareElementType(a, b ElementType) int {
	return strings.Compare(a.Id().String(), b.Id().String())
}

func compareElementType(a, b *elementType) int {
	return strings.Compare(a.Id().String(), b.Id().String())
}

////////////////////////////////////////////////////////////////////////////////

type externalObjectType struct {
	name    string
	trigger *elementType
	foreign bool
}

var _ ExternalObjectType = (*externalObjectType)(nil)
var _externalObjectType = utils.CastPointer[ExternalObjectType, externalObjectType]

func newExternalObjectType(name string, trigger *elementType, foreign bool) *externalObjectType {
	return &externalObjectType{
		name:    name,
		trigger: trigger,
		foreign: foreign,
	}
}

func (o *externalObjectType) Name() string {
	return o.name
}

func (o *externalObjectType) Trigger() ElementType {
	return _elementType(o.trigger)
}

func (o *externalObjectType) IsForeignControlled() bool {
	return o.foreign
}

////////////////////////////////////////////////////////////////////////////////

type internalObjectType struct {
	name   string
	phases map[Phase]*elementType
}

var _ InternalObjectType = (*internalObjectType)(nil)
var _internalObjectType = utils.CastPointer[InternalObjectType, internalObjectType]

func newInternalObjectType(name string, phases map[Phase]*elementType) *internalObjectType {
	return &internalObjectType{
		name:   name,
		phases: phases,
	}
}

func (o *internalObjectType) Name() string {
	return o.name
}

func (o *internalObjectType) Phases() []Phase {
	return utils.OrderedMapKeys(o.phases)
}

func (o *internalObjectType) Element(phase Phase) ElementType {
	return _elementType(o.phases[phase]) // avoid typed nil interface
}
