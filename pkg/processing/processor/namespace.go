package processor

import (
	"maps"
	"slices"
	"sync"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

type namespaceInfo struct {
	lock      sync.Mutex
	namespace model.NamespaceObject
	elements  map[ElementId]_Element
	internal  map[mmids.ObjectId]model.InternalObject

	pendingOperation func(lctx model.Logging, log logging.Logger) error
	pendingElements  map[ElementId]_Element
}

var _ Namespace = (*namespaceInfo)(nil)

func newNamespaceInfo(o internal.NamespaceObject) *namespaceInfo {
	return &namespaceInfo{
		namespace: o,
		elements:  map[ElementId]_Element{},
		internal:  map[mmids.ObjectId]model.InternalObject{},
	}
}

func (ni *namespaceInfo) GetNamespaceName() string {
	return ni.namespace.GetNamespaceName()
}

func (ni *namespaceInfo) Elements() []ElementId {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return utils.MapKeys(ni.elements)
}

func (ni *namespaceInfo) GetElement(id ElementId) Element {
	return ni._GetElement(id)
}

func (ni *namespaceInfo) _GetElement(id ElementId) _Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return ni.elements[id]
}

func (ni *namespaceInfo) _AddElement(i model.InternalObject, phase Phase) _Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	id := NewElementIdForPhase(i, phase)

	if e := ni.elements[id]; e != nil {
		return e
	}
	if f := ni.internal[id.ObjectId()]; f == nil {
		ni.internal[id.ObjectId()] = i
	} else {
		i = f
	}
	e := newElement(phase, i)
	ni.elements[id] = e
	return e
}

func (ni *namespaceInfo) tryLock(p *Processor, runid RunId) (bool, error) {
	ok, err := ni.namespace.TryLock(p.processingModel.ObjectBase(), runid)
	if ok {
		p.events.TriggerElementHandled(NewElementIdForPhase(ni.namespace, ""))
	}
	return ok, err
}

func (ni *namespaceInfo) clearElementLock(lctx model.Logging, log logging.Logger, p *Processor, elem _Element, rid RunId) error {
	// first: reset run id in in external objects
	err := p.updateRunId(lctx, log, "reset", elem, "")
	if err != nil {
		return err
	}
	// second, clear lock on internal object for given phase.
	ok, err := elem.Rollback(lctx, p.processingModel.ObjectBase(), rid)
	if err != nil {
		log.Error("releasing lock {{runid}} for element {{element}} failed", "element", elem.Id(), "error", err)
		return err
	}
	if ok {
		p.events.TriggerElementHandled(elem.Id())
		p.pending.Add(-1)
	}
	return nil
}

func (ni *namespaceInfo) clearLocks(lctx model.Logging, log logging.Logger, p *Processor) error {
	rid := ni.namespace.GetLock()
	if rid == "" {
		return nil
	}
	if len(ni.pendingElements) > 0 {
		log.Info("found pending {{amount}} locks for {{runid}}", "amount", len(ni.pendingElements))
		for eid, elem := range maps.Clone(ni.pendingElements) {
			err := ni.clearElementLock(lctx, log, p, elem, rid)
			if err == nil {
				delete(ni.pendingElements, eid)
			}
		}
		if len(ni.pendingElements) == 0 {
			_, err := ni.namespace.ClearLock(p.processingModel.ObjectBase(), rid)
			if err != nil {
				log.Info("releasing namespace lock {{runid}} failed")
				return err
			} else {
				log.Info("releasing namespace lock {{runid}} succeeded")
			}
			ni.pendingElements = nil
		} else {
			ni.pendingOperation = func(lctx model.Logging, log logging.Logger) error {
				return ni.clearLocks(lctx, log, p)
			}
		}
	}
	_, err := ni.namespace.ClearLock(p.processingModel.ObjectBase(), ni.namespace.GetLock())
	if err == nil {
		p.events.TriggerElementHandled(NewElementIdForPhase(ni.namespace, ""))
		log.Info("namespace {{namespace}} unlocked")
	}
	return err
}

func (ni *namespaceInfo) GetChildren(id ElementId) []Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return ni.getChildren(id)
}

func (ni *namespaceInfo) getChildren(id ElementId) []Element {
	var r []Element
	for _, e := range ni.elements {
		state := e.GetCurrentState()
		if state != nil {
			if slices.Contains(state.GetLinks(), id) {
				r = append(r, e)
			}
		}
	}
	return r
}

func (ni *namespaceInfo) list(typ string) []ElementId {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	var list []ElementId

	for _, e := range ni.elements {
		if typ == "" || e.GetType() == typ {
			list = append(list, e.Id())
		}
	}
	return list
}
