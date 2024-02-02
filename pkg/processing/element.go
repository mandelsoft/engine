package processing

import (
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
)

type ElementId = common.ElementId

type Element interface {
	common.Element

	GetLock() model.RunId
	GetCurrentState() CurrentState
	GetTargetState() TargetState
	SetTargetState(TargetState)

	TryLock(ob objectbase.Objectbase, id model.RunId) (bool, error)
	Rollback(lctx common.Logging, ob objectbase.Objectbase, id model.RunId) (bool, error)
	Commit(lctx common.Logging, ob objectbase.Objectbase, id model.RunId, commit *model.CommitInfo) (bool, error)
}

type element struct {
	id     ElementId
	object common.InternalObject

	runid common.RunId

	current CurrentState
	target  TargetState
}

var _ Element = (*element)(nil)

func NewElement(phase common.Phase, obj common.InternalObject) *element {
	e := &element{
		id:     common.NewElementId(obj.GetType(), obj.GetNamespace(), obj.GetName(), phase),
		object: obj,
		runid:  obj.GetLock(phase),
	}
	e.current = NewCurrentState(e)
	return e
}

func (e *element) GetType() string {
	return e.object.GetType()
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

func (e *element) GetPhase() common.Phase {
	return e.id.Phase()
}

func (e *element) GetObject() common.InternalObject {
	return e.object
}

func (e *element) GetLock() model.RunId {
	return e.object.GetLock(e.GetPhase())
}

func (e *element) GetCurrentState() CurrentState {
	return e.current
}

func (e *element) GetTargetState() TargetState {
	return e.target
}

func (e *element) SetTargetState(target TargetState) {
	e.target = target
}

func (e *element) TryLock(ob objectbase.Objectbase, id model.RunId) (bool, error) {
	return e.GetObject().TryLock(ob, e.id.Phase(), id)
}

func (e *element) Rollback(lctx common.Logging, ob objectbase.Objectbase, id model.RunId) (bool, error) {
	return e.GetObject().Rollback(lctx, ob, e.id.Phase(), id)
}

func (e *element) Commit(lctx common.Logging, ob objectbase.Objectbase, id model.RunId, commit *model.CommitInfo) (bool, error) {
	return e.GetObject().Commit(lctx, ob, e.id.Phase(), id, commit)
}
