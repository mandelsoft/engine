package processor

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/matcher"

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

	return maputils.Keys(ni.elements)
}

func (ni *namespaceInfo) GetElement(id ElementId) Element {
	return ni._GetElement(id)
}

func (ni *namespaceInfo) _GetElement(id ElementId) _Element {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return ni.elements[id]
}

func (ni *namespaceInfo) _getElement(id ElementId) _Element {
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

func (ni *namespaceInfo) filterElements(m matcher.Matcher[_Element]) map[ElementId]_Element {
	return maputils.FilterByValue(ni.elements, m)
}

func (ni *namespaceInfo) tryLock(r Reconcilation, runid RunId) (bool, error) {
	ok, err := ni.namespace.TryLock(r.Objectbase(), runid)
	if ok {
		r.TriggerNamespaceEvent(ni)
	}
	return ok, err
}

func (ni *namespaceInfo) clearElementLock(r Reconcilation, elem _Element, rid RunId) error {
	// first: reset run id in in external objects
	err := r.Controller().updateRunId(r, "reset", elem, "")
	if err != nil {
		return err
	}
	// second, clear lock on internal object for given phase.
	ok, err := elem.Rollback(r.LoggingContext(), r.Objectbase(), rid, false)
	if err != nil {
		r.Error("releasing lock {{runid}} for element {{element}} failed", "element", elem.Id(), "error", err)
		return err
	}
	if ok {
		r.TriggerElementEvent(elem)
		r.Controller().pending.Add(-1)
	}
	return nil
}

func (ni *namespaceInfo) clearLocks(r Reconcilation) error {
	rid := ni.namespace.GetLock()
	if rid == "" {
		return nil
	}
	if len(ni.pendingElements) > 0 {
		r.Info("found pending {{amount}} locks for {{runid}}", "amount", len(ni.pendingElements))
		for eid, elem := range maps.Clone(ni.pendingElements) {
			err := ni.clearElementLock(r, elem, rid)
			if err == nil {
				delete(ni.pendingElements, eid)
				r.TriggerElementEvent(elem)
			}
		}
		if len(ni.pendingElements) == 0 {
			_, err := ni.namespace.ClearLock(r.Objectbase(), rid)
			if err != nil {
				r.Info("releasing namespace lock {{runid}} failed")
				return err
			} else {
				r.Info("releasing namespace lock {{runid}} succeeded")
			}
			r.TriggerNamespaceEvent(ni)
			ni.pendingElements = nil
		} else {
			ni.pendingOperation = func(lctx model.Logging, log logging.Logger) error {
				return ni.clearLocks(r)
			}
		}
	}

	if IsObjectLock(ni.namespace.GetLock()) != nil {
		return nil
	}
	return ni.clearLock(r, ni.namespace.GetLock())
}

func (ni *namespaceInfo) clearLock(r Reconcilation, rid RunId) error {
	cur := ni.namespace.GetLock()
	if rid != cur {
		return nil
	}
	_, err := ni.namespace.ClearLock(r.Objectbase(), rid)
	if err == nil {
		r.Info("namespace {{namespace}} unlocked", "namespace", ni.namespace.GetNamespaceName())
		r.TriggerNamespaceEvent(ni)
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

func (ni *namespaceInfo) assureSlaves(log logging.Logger, p *Controller, check model.SlaveCheckFunction, update model.SlaveUpdateFunction, runid RunId, eids ...ElementId) error {
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

func (ni *namespaceInfo) setupElements(log logging.Logger, p *Controller, i model.InternalObject, phase Phase, runid RunId) _Element {
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

func (ni *namespaceInfo) RemoveInternal(log logging.Logger, m *processingModel, oid ObjectId) bool {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	for _, ph := range m.MetaModel().Phases(oid.GetType()) {
		log.Info(" - deleting phase {{phase}}", "phase", ph)
		delete(ni.elements, NewElementIdForPhase(oid, ph))
	}
	delete(ni.internal, oid)
	return len(ni.internal) == 0
}

func (ni *namespaceInfo) LockGraph(r Reconcilation, elem _Element) (*RunId, error) {
	id := NewRunId()

	if !ni.lock.TryLock() {
		return nil, nil
	}
	defer ni.lock.Unlock()

	log := r.WithValues("runid", id)
	ok, err := ni.tryLock(r, id)
	if err != nil {
		log.Info("locking namespace {{namespace}} for new runid {{runid}} failed", "error", err)
		return nil, err
	}
	if !ok {
		log.Info("cannot lock namespace {{namespace}} already locked for {{current}}", "current", ni.namespace.GetLock())
		return nil, nil
	}
	log.Info("namespace {{namespace}} locked for new runid {{runid}}")
	defer func() {
		err := ni.clearLocks(r)
		if err != nil {
			log.Error("cannot clear namespace lock for {{namespace}} -> requeue", "error", err)
			r.Controller().EnqueueNamespace(ni.GetNamespaceName())
		}
	}()

	ok, err = ni.doLockGraph(r, id, false, elem)
	if !ok || err != nil {
		return nil, err
	}
	return &id, nil
}

func (ni *namespaceInfo) doLockGraph(r Reconcilation, runid RunId, keep bool, candidates ..._Element) (bool, error) {
	elems := NewOrderedElementSet()
	for _, elem := range candidates {
		ok, err := ni._tryLockGraph(r, runid, elem, elems)
		if !ok || err != nil {
			return false, err
		}
		ok, err = ni._lockGraph(r, runid, keep, elems)
		if !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}

func (ni *namespaceInfo) _tryLockGraph(r Reconcilation, runid RunId, elem _Element, elems OrderedElementSet) (bool, error) {
	if !elems.Has(elem.Id()) {
		cur := elem.GetLock()
		if cur != "" && cur != runid {
			r.Info("element {{candidate}} already locked for {{lock}}", "candidate", elem.Id(), "lock", cur)
			return false, nil
		}
		elems.Add(elem)

		for _, d := range ni.getChildren(elem.Id()) {
			ok, err := ni._tryLockGraph(r, runid, d.(_Element), elems)
			if !ok || err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (ni *namespaceInfo) _lockGraph(r Reconcilation, id RunId, keep bool, elems OrderedElementSet) (bool, error) {
	var ok bool
	var err error

	ni.pendingElements = map[ElementId]_Element{}

	r.Debug("found {{amount}} elements in graph", "amount", elems.Size())
	for _, elem := range elems.Order() {
		r.Debug("locking {{nestedelem}}", "nestedelem", elem.Id())
		ok, err = elem.TryLock(r.Objectbase(), id)
		if err != nil {
			r.Debug("locking failed for {{nestedelem}}", "nestedelem", elem.Id(), "error", err)
			return false, err
		}
		if !keep {
			ni.pendingElements[elem.Id()] = elem
		}
		if ok {
			// log.Debug("successfully locked {{nestedelem}}", "nestedelem", elem.Id())
			r.Controller().events.TriggerElementEvent(elem)
			r.Controller().pending.Add(1)
		}
	}
	ni.pendingElements = nil
	return true, nil
}
