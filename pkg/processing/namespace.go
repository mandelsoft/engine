package processing

import (
	"maps"
	"sync"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/logging"
)

type NamespaceInfo struct {
	lock      sync.Mutex
	namespace common.Namespace
	elements  map[ElementId]Element
	internal  map[common.ObjectId]common.InternalObject

	pendingOperation func(log logging.Logger) error
	pendingElements  map[ElementId]Element
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

func (ni *NamespaceInfo) AddElement(i model.InternalObject, phase model.Phase) Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	id := common.NewElementIdForPhase(i, phase)

	if e := ni.elements[id]; e != nil {
		return e
	}
	if f := ni.internal[id.ObjectId()]; f == nil {
		ni.internal[id.ObjectId()] = i
	} else {
		i = f
	}
	e := NewElement(phase, i)
	ni.elements[e.id] = e
	return e
}

func (ni *NamespaceInfo) ClearLocks(log logging.Logger, p *Processor) error {
	ni.lock.Lock()
	defer ni.lock.Unlock()
	return ni.clearLocks(log, p)
}

func (ni *NamespaceInfo) clearElementLock(log logging.Logger, p *Processor, elem Element, rid model.RunId) error {
	// first: reset run id in in external objects
	err := p.updateRunId(log, "reset", elem, "")
	if err != nil {
		return err
	}
	// second, clear lock on internal object for given phase.
	_, err = elem.ClearLock(p.ob, rid)
	if err != nil {
		log.Error("releasing lock {{runid}} for element {{element}} failed", "element", elem.Id(), "error", err)
		return err
	}
	return nil
}

func (ni *NamespaceInfo) clearLocks(log logging.Logger, p *Processor) error {
	rid := ni.namespace.GetLock()
	if rid == "" {
		return nil
	}
	if len(ni.pendingElements) > 0 {
		log.Info("found pending {{amount}} locks for {{runid}}", "amount", len(ni.pendingElements))
		for eid, elem := range maps.Clone(ni.pendingElements) {
			err := ni.clearElementLock(log, p, elem, rid)
			if err == nil {
				delete(ni.pendingElements, eid)
			}
		}
		if len(ni.pendingElements) == 0 {
			_, err := ni.namespace.ClearLock(p.ob, rid)
			if err != nil {
				log.Info("releasing namespace lock {{runid}} failed")
				return err
			} else {
				log.Info("releasing namespace lock {{runid}} succeeded")
			}
			ni.pendingElements = nil
		}
	}
	return nil
}
