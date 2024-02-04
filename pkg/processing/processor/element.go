package processor

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/model"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
)

type _Element interface {
	Element

	GetLock() RunId
	GetCurrentState() CurrentState
	GetTargetState() TargetState
	SetTargetState(TargetState)

	TryLock(ob objectbase.Objectbase, id RunId) (bool, error)
	Rollback(lctx model.Logging, ob objectbase.Objectbase, id RunId) (bool, error)
	Commit(lctx model.Logging, ob objectbase.Objectbase, id RunId, commit *model.CommitInfo) (bool, error)
}

type element struct {
	id     ElementId
	object model.InternalObject

	runid RunId

	current CurrentState
	target  TargetState
}

var _ _Element = (*element)(nil)
var _element = utils.CastPointer[Element, element]

func newElement(phase Phase, obj model.InternalObject) *element {
	e := &element{
		id:     NewElementId(obj.GetType(), obj.GetNamespace(), obj.GetName(), phase),
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

func (e *element) GetPhase() Phase {
	return e.id.GetPhase()
}

func (e *element) GetObject() model.InternalObject {
	return e.object
}

func (e *element) GetLock() RunId {
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

func (e *element) TryLock(ob objectbase.Objectbase, id RunId) (bool, error) {
	return e.GetObject().TryLock(ob, e.id.GetPhase(), id)
}

func (e *element) Rollback(lctx model.Logging, ob objectbase.Objectbase, id RunId) (bool, error) {
	return e.GetObject().Rollback(lctx, ob, e.id.GetPhase(), id)
}

func (e *element) Commit(lctx model.Logging, ob objectbase.Objectbase, id RunId, commit *model.CommitInfo) (bool, error) {
	return e.GetObject().Commit(lctx, ob, e.id.GetPhase(), id, commit)
}
