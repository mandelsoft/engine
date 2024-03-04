package processor

import (
	"fmt"
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
	return ni._addElement(i, phase)
}

func (ni *namespaceInfo) _addElement(i model.InternalObject, phase Phase) _Element {
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
		p.events.TriggerNamespaceEvent(ni)
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
	ok, err := elem.Rollback(lctx, p.processingModel.ObjectBase(), rid, false)
	if err != nil {
		log.Error("releasing lock {{runid}} for element {{element}} failed", "element", elem.Id(), "error", err)
		return err
	}
	if ok {
		p.events.TriggerElementEvent(elem)
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
				p.events.TriggerElementEvent(elem)
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
			p.events.TriggerNamespaceEvent(ni)
			ni.pendingElements = nil
		} else {
			ni.pendingOperation = func(lctx model.Logging, log logging.Logger) error {
				return ni.clearLocks(lctx, log, p)
			}
		}
	}
	_, err := ni.namespace.ClearLock(p.processingModel.ObjectBase(), ni.namespace.GetLock())
	if err == nil {
		log.Info("namespace {{namespace}} unlocked")
		p.events.TriggerNamespaceEvent(ni)
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
			if e.GetStatus() != model.STATUS_DELETED && slices.Contains(state.GetLinks(), id) {
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

func (ni *namespaceInfo) assureSlaves(log logging.Logger, p *Processor, check model.SlaveCheckFunction, update model.SlaveUpdateFunction, runid RunId, eids ...ElementId) error {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	// first, check existing objects
	for _, eid := range eids {
		if !p.processingModel.MetaModel().HasElementType(eid.TypeId()) {
			return fmt.Errorf("unknown element type %q for slave", eid.TypeId())
		}
		e := ni.elements[eid]
		if e != nil && check != nil {
			err := check(e.GetObject())
			if err != nil {
				return err
			}
		}
	}

	// second, update/create required objects
	for _, eid := range eids {
		e := ni.elements[eid]
		if e == nil {
			i, err := update(p.processingModel.ObjectBase(), eid, nil)
			if err != nil {
				return err
			}
			_, err = i.AddFinalizer(p.processingModel.ObjectBase(), FINALIZER)
			if err != nil {
				return err
			}
			e = ni.setupElements(log, p, i, eid.GetPhase(), runid)
		}
		// always trigger new elements, because they typically have no correct current state dependencies.
		// Those dependencies are configured in form of a state change.
		// (The internal slave objects keeps dependencies as additional state enriching the object state
		// of the external object)
		p.Enqueue(CMD_ELEM, e)
	}
	return nil
}

func (ni *namespaceInfo) setupElements(log logging.Logger, p *Processor, i model.InternalObject, phase Phase, runid RunId) _Element {
	var elem _Element
	log.Info("setup new internal object {{id}} for required phase {{reqphase}}", "id", NewObjectIdFor(i), "reqphase", phase)
	tolock, _ := p.processingModel.MetaModel().GetDependentTypePhases(NewTypeId(i.GetType(), phase))
	for _, ph := range p.processingModel.MetaModel().Phases(i.GetType()) {
		n := ni._addElement(i, ph)
		log.Info("  setup new phase {{newelem}}", "newelem", n.Id())
		if ph == phase {
			elem = n
		}
		if slices.Contains(tolock, ph) {
			ok, err := n.TryLock(p.processingModel.ObjectBase(), runid)
			if !ok { // new object should already be locked correctly provide atomic phase creation
				panic(fmt.Sprintf("cannot lock incorrectly locked new element: %s", err))
			}
			log.Info("  dependent phase {{depphase}} locked", "depphase", ph)
			p.pending.Add(1)
		}
	}
	return elem
}
