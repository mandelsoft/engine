package processing

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type ElementId = common.ElementId

type Element interface {
	common.Element

	GetCurrentState() State
	GetTargetState() State
}

type element struct {
	id     ElementId
	object *common.InternalObject

	runid common.RunId

	current State
	target  State
}

var _ Element = (*element)(nil)

func NewElement(phase common.Phase, obj *common.InternalObject) *element {
	e := &element{
		id:     common.NewElementId((*obj).GetType(), (*obj).GetNamespace(), (*obj).GetName(), phase),
		object: obj,
		runid:  (*obj).GetLock(phase),
	}
	e.current = NewState(e, ObjectState)

	if (*obj).GetLock(phase) != "" {
		t := (*obj).GetTargetState(phase)
		if t != nil {
			e.target = NewState(e, ObjectTargetState)
		}
	}
	return e
}

func (e *element) GetNamespace() string {
	return (*e.object).GetNamespace()
}

func (e *element) Id() ElementId {
	return e.id
}

func (e element) GetPhase() common.Phase {
	return e.id.Phase()
}

func (e *element) GetObject() *common.InternalObject {
	return e.object
}

func (e element) GetCurrentState() State {
	return e.current
}

func (e element) GetTargetState() State {
	return e.target
}
