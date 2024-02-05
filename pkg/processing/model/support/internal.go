package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ElementLocks struct {
	ElementLocks map[mmids.Phase]mmids.RunId `json:"locks,omitempty"`
}

func (n *ElementLocks) ClearLock(phase mmids.Phase, id mmids.RunId) bool {
	if len(n.ElementLocks) == 0 {
		return false
	}
	if n.ElementLocks[phase] != id {
		return false
	}
	delete(n.ElementLocks, phase)
	return true
}

func (n *ElementLocks) GetLock(phase mmids.Phase) mmids.RunId {
	if len(n.ElementLocks) == 0 {
		return ""
	}
	return n.ElementLocks[phase]
}

func (n *ElementLocks) TryLock(phase mmids.Phase, id mmids.RunId) bool {
	if len(n.ElementLocks) != 0 && n.ElementLocks[phase] != "" && n.ElementLocks[phase] != id {
		return false
	}
	if n.ElementLocks == nil {
		n.ElementLocks = map[mmids.Phase]mmids.RunId{}
	}
	n.ElementLocks[phase] = id
	return true
}

type InternalDBObjectSupport struct {
	database.GenerationObjectMeta

	ElementLocks
}

////////////////////////////////////////////////////////////////////////////////

type InternalDBObject interface {
	DBObject

	GetLock(phase mmids.Phase) mmids.RunId
	TryLock(phase mmids.Phase, id mmids.RunId) bool
	ClearLock(phase mmids.Phase, id mmids.RunId) bool
}

////////////////////////////////////////////////////////////////////////////////

type InternalObject interface {
	model.InternalObject
	GetBase() DBObject
}

type InternalObjectSupport struct { // cannot use struct type here (Go)
	Lock sync.Mutex
	Wrapper
}

// GetExternalState is a default implementation just forwarding
// the external state as provided by the external object.
func (n *InternalObjectSupport) GetExternalState(o model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return o.GetState()
}

func (n *InternalObjectSupport) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *InternalObjectSupport) GetDBObject() InternalDBObject {
	return utils.Cast[InternalDBObject](n.GetBase())
}

func (n *InternalObjectSupport) GetLock(phase mmids.Phase) mmids.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetLock(phase)
}

func (n *InternalObjectSupport) TryLock(ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

type Committer interface {
	Commit(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo)
}

type CommitFunc func(lctx model.Logging, o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo)

func (f CommitFunc) Commit(lctx model.Logging, o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo) {
	f(lctx, o, phase, spec)
}

func (n *InternalObjectSupport) Commit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo, committer Committer) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		if b && commit != nil {
			committer.Commit(lctx, o, phase, commit)
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}
