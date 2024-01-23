package processing

import (
	common2 "github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
)

type ElementId = common2.ElementId

type Element interface {
	common2.Element

	GetLock() model.RunId
	GetCurrentState() State
	GetTargetState() State

	ClearLock(ob objectbase.Objectbase, id model.RunId) (bool, error)
	TryLock(ob objectbase.Objectbase, id model.RunId) (bool, error)
}

type element struct {
	id     ElementId
	object common2.InternalObject

	runid common2.RunId

	current State
	target  State
}

var _ Element = (*element)(nil)

func NewElement(phase common2.Phase, obj common2.InternalObject) *element {
	e := &element{
		id:     common2.NewElementId(obj.GetType(), obj.GetNamespace(), obj.GetName(), phase),
		object: obj,
		runid:  obj.GetLock(phase),
	}
	e.current = NewState(e, ObjectState)

	if obj.GetLock(phase) != "" {
		t := obj.GetTargetState(phase)
		if t != nil {
			e.target = NewState(e, ObjectTargetState)
		}
	}
	return e
}

func (e *element) GetName() string {
	return e.object.GetName()
}

func (e *element) GetNamespace() string {
	return e.object.GetNamespace()
}

func (e *element) Id() ElementId {
	return e.id
}

func (e element) GetPhase() common2.Phase {
	return e.id.Phase()
}

func (e *element) GetObject() common2.InternalObject {
	return e.object
}

func (e *element) GetLock() model.RunId {
	return e.object.GetLock(e.GetPhase())
}

func (e *element) GetCurrentState() State {
	return e.current
}

func (e *element) GetTargetState() State {
	return e.target
}

func (e *element) ClearLock(ob objectbase.Objectbase, id model.RunId) (bool, error) {
	return e.GetObject().ClearLock(ob, e.id.Phase(), id)
}

func (e *element) TryLock(ob objectbase.Objectbase, id model.RunId) (bool, error) {
	return e.GetObject().TryLock(ob, e.id.Phase(), id)
}
