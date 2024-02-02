package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ElementLocks struct {
	ElementLocks map[model.Phase]model.RunId `json:"locks,omitempty"`
}

func (n *ElementLocks) ClearLock(phase model.Phase, id model.RunId) bool {
	if len(n.ElementLocks) == 0 {
		return false
	}
	if n.ElementLocks[phase] != id {
		return false
	}
	delete(n.ElementLocks, phase)
	return true
}

func (n *ElementLocks) GetLock(phase model.Phase) model.RunId {
	if len(n.ElementLocks) == 0 {
		return ""
	}
	return n.ElementLocks[phase]
}

func (n *ElementLocks) TryLock(phase model.Phase, id model.RunId) bool {
	if len(n.ElementLocks) != 0 && n.ElementLocks[phase] != "" && n.ElementLocks[phase] != id {
		return false
	}
	if n.ElementLocks == nil {
		n.ElementLocks = map[model.Phase]model.RunId{}
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

	GetLock(phase model.Phase) model.RunId
	TryLock(phase model.Phase, id model.RunId) bool
	ClearLock(phase model.Phase, id model.RunId) bool
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

func (n *InternalObjectSupport) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *InternalObjectSupport) GetDBObject() InternalDBObject {
	return utils.Cast[InternalDBObject](n.GetBase())
}

func (n *InternalObjectSupport) GetLock(phase common.Phase) common.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetLock(phase)
}

func (n *InternalObjectSupport) TryLock(ob objectbase.Objectbase, phase common.Phase, id common.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport) Rollback(lctx common.Logging, ob objectbase.Objectbase, phase common.Phase, id model.RunId) (bool, error) {
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
	Commit(lctx common.Logging, _o InternalDBObject, phase model.Phase, spec *model.CommitInfo)
}

type CommitFunc func(lctx common.Logging, o InternalDBObject, phase model.Phase, spec *model.CommitInfo)

func (f CommitFunc) Commit(lctx common.Logging, o InternalDBObject, phase model.Phase, spec *model.CommitInfo) {
	f(lctx, o, phase, spec)
}

func (n *InternalObjectSupport) Commit(lctx common.Logging, ob objectbase.Objectbase, phase model.Phase, id model.RunId, commit *model.CommitInfo, committer Committer) (bool, error) {
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
