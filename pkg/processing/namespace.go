package processing

import (
	"maps"
	"slices"
	"sync"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/logging"
)

type Namespace = common.Namespace

type namespaceInfo struct {
	lock      sync.Mutex
	namespace common.NamespaceObject
	elements  map[ElementId]Element
	internal  map[common.ObjectId]common.InternalObject

	pendingOperation func(log logging.Logger) error
	pendingElements  map[ElementId]Element
}

var _ Namespace = (*namespaceInfo)(nil)

func newNamespaceInfo(o common.NamespaceObject) *namespaceInfo {
	return &namespaceInfo{
		namespace: o,
		elements:  map[ElementId]Element{},
		internal:  map[common.ObjectId]common.InternalObject{},
	}
}

func (ni *namespaceInfo) GetNamespaceName() string {
	return ni.namespace.GetNamespaceName()
}

func (ni *namespaceInfo) GetElement(id ElementId) common.Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return ni.elements[id]
}

func (ni *namespaceInfo) AddElement(i model.InternalObject, phase model.Phase) Element {
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

func (ni *namespaceInfo) clearElementLock(lctx common.Logging, log logging.Logger, p *Processor, elem Element, rid model.RunId) error {
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
		p.pending.Add(-1)
	}
	return nil
}

func (ni *namespaceInfo) clearLocks(lctx common.Logging, log logging.Logger, p *Processor) error {
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
		}
	}
	_, err := ni.namespace.ClearLock(p.processingModel.ObjectBase(), ni.namespace.GetLock())
	return err
}

func (ni *namespaceInfo) GetChildren(id ElementId) []common.Element {
	var r []common.Element
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
