package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
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
	if len(n.ElementLocks) != 0 && n.ElementLocks[phase] != "" {
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

type InternalDBObject interface {
	DBObject

	GetLock(phase model.Phase) model.RunId

	ClearLock(phase model.Phase, id model.RunId) bool
	TryLock(phase model.Phase, id model.RunId) bool
}

////////////////////////////////////////////////////////////////////////////////

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

func (n *InternalObjectSupport) ClearLock(ob objectbase.Objectbase, phase common.Phase, id model.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	db := n.GetDatabase(ob)
	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).ClearLock(phase, id)
		return b, b
	}

	o := n.GetBase()
	r, err := database.Modify(db, &o, mod)
	n.SetBase(o)
	return r, err
}

func (n *InternalObjectSupport) TryLock(ob objectbase.Objectbase, phase common.Phase, id common.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	db := n.GetDatabase(ob)
	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}

	o := n.GetBase()
	r, err := database.Modify(db, &o, mod)
	n.SetBase(o)
	return r, err
}
