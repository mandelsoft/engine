package _default

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type ElementLocks struct {
	Lock         sync.Mutex                    `json:"-"`
	ElementLocks map[common.Phase]common.RunId `json:"locks,omitempty"`
}

func (n *ElementLocks) ClearLock(phase common.Phase) {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	delete(n.ElementLocks, phase)
}

func (n *ElementLocks) GetLock(phase common.Phase) common.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.ElementLocks[phase]
}

func (n *ElementLocks) TryLock(phase common.Phase, id common.RunId) bool {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	if n.ElementLocks[phase] != "" {
		return false
	}
	n.ElementLocks[phase] = id
	return true
}

type InternalObject struct {
	database.GenerationObjectMeta

	ElementLocks
}
