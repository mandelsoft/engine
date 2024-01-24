package metamodel

import (
	"slices"
	"sort"
	"strings"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/utils"
)

const DEFAULT_PHASE = common.Phase("PhaseUpdating")

///////////////////////////////////////////////////////////////////////////////

type ElementType interface {
	Id() TypeId

	Dependencies() []ElementType
	TriggeredBy() []string

	addDependency(d ElementType)
	addTrigger(t string)
}

type elementType struct {
	id           TypeId
	dependencies []ElementType
	triggers     []string
}

var _ ElementType = (*elementType)(nil)

func newElementType(objtype string, phase common.Phase) *elementType {
	return &elementType{
		id: common.NewTypeId(objtype, phase),
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

func (e *elementType) addTrigger(typ string) {
	if !slices.Contains(e.triggers, typ) {
		e.triggers = append(e.triggers, typ)
		sort.Strings(e.triggers)
	}
}

func (e *elementType) Dependencies() []ElementType {
	return slices.Clone(e.dependencies)
}

func (e *elementType) TriggeredBy() []string {
	return slices.Clone(e.triggers)
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

	Phases() []common.Phase
	Element(common.Phase) ElementType
}

type internalObjectType struct {
	name   string
	phases map[common.Phase]ElementType
}

var _ InternalObjectType = (*internalObjectType)(nil)

func newInternalObjectType(name string, phases map[common.Phase]ElementType) *internalObjectType {
	return &internalObjectType{
		name:   name,
		phases: phases,
	}
}

func (o *internalObjectType) Name() string {
	return o.name
}

func (o *internalObjectType) Phases() []common.Phase {
	return utils.OrderedMapKeys(o.phases)
}

func (o *internalObjectType) Element(phase common.Phase) ElementType {
	return o.phases[phase]
}
