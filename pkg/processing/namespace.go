package processing

import (
	"maps"
	"sync"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/logging"
)

type NamespaceInfo struct {
	lock      sync.Mutex
	namespace common.Namespace
	elements  map[ElementId]Element
	internal  map[common.ObjectId]common.InternalObject

	pendingLocks map[ElementId]Element
}

func NewNamespaceInfo(ns common.Namespace) *NamespaceInfo {
	return &NamespaceInfo{
		namespace: ns,
		elements:  map[ElementId]Element{},
		internal:  map[common.ObjectId]common.InternalObject{},
	}
}

func (ni *NamespaceInfo) GetNamespaceName() string {
	return ni.namespace.GetNamespaceName()
}

func (ni *NamespaceInfo) ClearLocks(log logging.Logger, p *Processor) error {
	ni.lock.Lock()
	defer ni.lock.Unlock()
	return ni.clearLocks(log, p)
}

func (ni *NamespaceInfo) clearLocks(log logging.Logger, p *Processor) error {
	rid := ni.namespace.GetLock()
	if rid == "" {
		return nil
	}
	if len(ni.pendingLocks) > 0 {
		log.Info("found pending {{amount}} locks for {{runid}}", "amount", len(ni.pendingLocks))
		for eid, elem := range maps.Clone(ni.pendingLocks) {
			_, err := elem.ClearLock(p.ob, rid)
			if err != nil {
				log.Info("releasing lock {{runid}} for element {{element}} failed", "element", elem.Id())
			} else {
				delete(ni.pendingLocks, eid)
			}
		}
		if len(ni.pendingLocks) == 0 {
			_, err := ni.namespace.ClearLock(p.ob, rid)
			if err != nil {
				log.Info("releasing namespace lock {{runid}} failed")
				return err
			} else {
				log.Info("releasing namespace lock {{runid}} succeeded")
			}
			ni.pendingLocks = nil
		}
	}
	return nil
}
