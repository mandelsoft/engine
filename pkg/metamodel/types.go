package metamodel

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/utils"
)

const DEFAULT_PHASE = common.Phase("PhaseUpdating")

type Encoding = common.Encoding
type Phase = common.Phase
type ElementId = common.ElementId
type ObjectId = common.ObjectId

type TypeId struct {
	objtype string
	phase   Phase
}

func NewTypeId(typ string, phase Phase) TypeId {
	return TypeId{
		objtype: typ,
		phase:   phase,
	}
}

func (o TypeId) Type() string {
	return o.objtype
}

func (o TypeId) Phase() Phase {
	return o.phase
}

func (o TypeId) String() string {
	return fmt.Sprintf("%s:%s", o.objtype, o.phase)
}

type _typeidId = TypeId

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

func newElementType(objtype string, phase common.Phase) *elementType {
	return &elementType{
		id: NewTypeId(objtype, phase),
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
