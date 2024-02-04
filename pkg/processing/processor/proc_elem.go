package processor

import (
	"errors"
	"fmt"
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processElement(lctx model.Logging, cmd string, id ElementId) pool.Status {
	nctx := lctx.WithValues("namespace", id.GetNamespace(), "element", id).WithName(id.String())
	elem := p.processingModel._GetElement(id)
	if elem == nil {
		if cmd != CMD_EXT {
			return pool.StatusFailed(fmt.Errorf("unknown element %q", id))
		}
		var status pool.Status
		elem, status = p.handleNew(nctx, id)
		if elem == nil {
			return status
		}
	}

	runid := elem.GetLock()
	if runid == "" {
		if cmd == CMD_EXT {
			return p.handleExternalChange(nctx, elem)
		}
	} else {
		lctx = lctx.WithValues("namespace", id.GetNamespace(), "element", id, "runid", runid).WithName(string(runid)).WithName(elem.Id().String())
		return p.handleRun(lctx, elem)
	}
	return pool.StatusCompleted()
}

func (p *Processor) handleNew(lctx model.Logging, id ElementId) (_Element, pool.Status) {
	log := lctx.Logger()
	log.Info("processing new element {{element}}")
	_i, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return nil, pool.StatusCompleted(err)
		}
		_i, err = p.processingModel.ObjectBase().CreateObject(id)
		if err != nil {
			return nil, pool.StatusCompleted(err)
		}
	}
	i := _i.(model.InternalObject)

	ni, err := p.processingModel.AssureNamespace(log, i.GetNamespace(), true)
	if err != nil {
		return nil, pool.StatusCompleted(err)
	}

	return ni._AddElement(i, id.GetPhase()), pool.StatusCompleted()
}

type Value struct {
	msg string
}

func (p *Processor) handleExternalChange(lctx model.Logging, e _Element) pool.Status {
	log := lctx.Logger()
	log.Info("processing external element trigger for {{element}}")
	types := p.processingModel.MetaModel().GetTriggeringTypesForElementType(e.Id().TypeId())
	if len(types) > 0 {
		log.Info("checking state of external objects for element {{element}}")
		changed := false
		cur := e.GetCurrentState().GetObjectVersion()
		for _, t := range types {
			id := database.NewObjectId(t, e.GetNamespace(), e.GetName())
			log := log.WithValues("extid", id)
			_o, err := p.processingModel.ObjectBase().GetObject(id)
			if err != nil {
				lctx.Logger().Error("cannot get external object {{extid}}")
				continue
			}

			o := _o.(model.ExternalObject)
			v := o.GetState().GetVersion()
			if v == cur {
				log.Info("state of external object {{extid}} not changed ({{version}})", "version", v)
			} else {
				changed = true
				log.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
			}
		}
		if changed {
			log.Info("trying to initiate new run for {{element}}")

			rid, err := p.lockGraph(lctx, log, e)
			if err == nil {
				if rid != nil {
					log.Info("starting run {{runid}}", "runid", *rid)
					return pool.StatusRedo()
				} else {
					err = fmt.Errorf("delay initiation of new run")
				}
			}
			return pool.StatusCompleted(err)
		}
	}
	return pool.StatusCompleted()
}

func (p *Processor) handleRun(lctx model.Logging, e _Element) pool.Status {
	log := lctx.Logger()
	ni := p.getNamespace(e.GetNamespace())

	var missing, waiting []ElementId
	var inputs model.Inputs

	log.Info("processing element {{element}}")

	var links []ElementId
	if e.GetTargetState() == nil {
		// check current dependencies (target state not yet fixed)
		log.Info("checking current links")
		links = e.GetCurrentState().GetLinks()

		for _, l := range links {
			if !p.processingModel.MetaModel().HasDependency(e.Id().TypeId(), l.TypeId()) {
				return pool.StatusFailed(fmt.Errorf("invalid dependency from %s to %s", e.Id().TypeId(), l.TypeId()))
			}
		}
		missing, waiting, inputs = p.checkReady(ni, links)

		// first check current state
		if p.notifyCurrentWaitingState(lctx, log, e, missing, waiting, inputs) {
			return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
		}
		// fix target state by transferring the current external state to the internal object
		err := p.hardenTargetState(lctx, log, e)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		// checking target dependencies after fixing the target state
		log.Info("checking target links and get actual inputs")
		links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
		for _, l := range links {
			if !p.processingModel.MetaModel().HasDependency(e.Id().TypeId(), l.TypeId()) {
				return pool.StatusFailed(fmt.Errorf("invalid dependency from %s to %s", e.Id().TypeId(), l.TypeId()))
			}
		}
	} else {
		links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
	}

	missing, waiting, inputs = p.checkReady(ni, links)
	if p.notifyTargetWaitingState(lctx, log, e, missing, waiting, inputs) {
		return pool.StatusCompleted(fmt.Errorf("still waiting for effective predecessors"))
	}

	target := e.GetObject().GetTargetState(e.GetPhase())

	// check effective version for required phase processing.
	if target.GetInputVersion(inputs) == e.GetCurrentState().GetInputVersion() &&
		target.GetObjectVersion() == e.GetCurrentState().GetObjectVersion() {
		log.Info("effective version unchanged -> skip processing of phase")
		err := p.notifyCompletedState(lctx, log, ni, e, "no processing required", nil)
		return pool.StatusCompleted(err)
	}

	if e.GetTargetState() == nil {
		// mark element to be ready by setting the elements target state to the target state of the internal
		// object for the actual phase
		e.SetTargetState(NewTargetState(e))
	}

	// now we can process the phase
	log.Info("executing phase {{phase}} of internal object {{intid}}", "phase", e.GetPhase(), "intid", e.Id().ObjectId())
	status := e.GetObject().Process(model.Request{
		Logging:       lctx,
		Model:         p.processingModel,
		Element:       e,
		Inputs:        inputs,
		ElementAccess: ni,
	})

	if status.Error != nil {
		p.updateStatus(lctx, log, e, status.Status, status.Error.Error())
		if status.Status == model.STATUS_FAILED {
			// non-recoverable error, wait for new change in external object state
			log.Error("processing provides non recoverable error", "error", status.Error)
			return pool.StatusFailed(status.Error)
		}
		log.Error("processing provides error", "error", status.Error)
		return pool.StatusCompleted(status.Error)
	} else {
		// if no error is provided, check for requested object creation.
		// an execution might provide new internal objects.
		// this objects MUST already be configured with the required links,
		// especially the one to the current element.
		// If processed, those object MUST create the matching external object.
		for _, c := range status.Creation {
			if p.processingModel.MetaModel().GetInternalType(c.Internal.GetType()) == nil {
				log.Error("skipping creation of requested object for unknown internal type {{type}}", "type", c.Internal.GetType())
				continue
			}
			n := p.setupNewInternalObject(log, ni, c.Internal, c.Phase, e.GetLock())
			p.pending.Add(1)
			// always trigger new elements, because they typically have no correct current state dependencies.
			// Those dependencies are configured in form of a state change.
			// (The internal slave objects keeps dependencies as additional state enriching the object state
			// of the external object)
			p.Enqueue(CMD_ELEM, n)
		}
		if status.Status == model.STATUS_DELETED {
			p.internalObjectDeleted(log, ni, e)
		}
		if status.Status == model.STATUS_COMPLETED || status.Status == model.STATUS_DELETED {
			err := p.notifyCompletedState(lctx, log, ni, e, "processing completed", inputs, status.ResultState, CalcEffectiveVersion(inputs, e.GetTargetState().GetObjectVersion()))
			if err != nil {
				return pool.StatusCompleted(err)
			}
			p.events.Completed(e.Id())
		}
	}
	return pool.StatusCompleted()
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) setupNewInternalObject(log logging.Logger, ni *namespaceInfo, i model.InternalObject, phase Phase, runid RunId) Element {
	var elem Element
	log.Info("setup new internal object {{id}} for required phase {{reqphase}}", "id", NewObjectIdFor(i), "reqphase", phase)
	tolock := p.processingModel.MetaModel().GetDependentTypePhases(NewTypeId(i.GetType(), phase))
	for _, ph := range p.processingModel.MetaModel().Phases(i.GetType()) {
		n := ni._AddElement(i, ph)
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
		}
	}
	return elem
}

func (p *Processor) internalObjectDeleted(log logging.Logger, ni *namespaceInfo, elem Element) {
	var children []ElementId
	log.Info("internal object {{elem}} deleted by processing step")
	for _, ph := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		for _, c := range ni.GetChildren(NewElementIdForPhase(elem, ph)) {
			if !slices.Contains(children, c.Id()) {
				children = append(children, c.Id())
			}
		}
	}
	for _, ph := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		delete(ni.elements, NewElementIdForPhase(elem, ph))
	}
	for _, c := range children {
		log.Info("  trigger dependent element {{depelem}}", "depelem", c)
		p.EnqueueKey(CMD_ELEM, c)
	}
}

func (p *Processor) notifyCompletedState(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, msg string, inputs model.Inputs, args ...interface{}) error {
	var ci *model.CommitInfo

	result := GetResultState(args...)
	target := e.GetTargetState()
	if result != nil {
		ci = &model.CommitInfo{
			InputVersion: target.GetInputVersion(inputs),
			State:        result,
		}
	}
	_, err := e.Commit(lctx, p.processingModel.ObjectBase(), e.GetLock(), ci)
	if err != nil {
		log.Error("cannot unlock element {{element}}", "error", err)
		return err
	}
	log.Info("completed processing of element {{element}}", "output")
	p.updateStatus(lctx, log, e, model.STATUS_COMPLETED, msg, append(args, RunId(""))...)
	p.pending.Add(-1)
	p.triggerChildren(log, ni, e, true)
	return nil
}

func (p *Processor) notifyCurrentWaitingState(lctx model.Logging, log logging.Logger, e Element, missing, waiting []ElementId, inputs model.Inputs) bool {
	var keys []interface{}
	if len(missing) > 0 {
		keys = append(keys, "missing", utils.Join(missing))
	}
	if len(inputs) > 0 {
		keys = append(keys, "found", utils.Join(utils.MapKeys(inputs)))
	}
	if len(waiting) > 0 {
		keys = append(keys, "waiting", utils.Join(waiting))
		log.Info("inputs not ready", keys...)
		if len(missing) > 0 {
			p.updateStatus(lctx, log, e, model.STATUS_WAITING, fmt.Sprintf("unresolved dependencies %s", utils.Join(missing)), nil, e.GetLock())
		} else {
			p.updateStatus(lctx, log, e, model.STATUS_PENDING, fmt.Sprintf("waiting for %s", utils.Join(waiting)), e.GetLock())
		}
		return true
	}
	if len(missing) > 0 {
		log.Info("found missing dependencies {{missing}}, but other dependencies ready {{found}} -> continue with target state", keys...)
	} else {
		log.Info("inputs according to current state ready", keys...)
	}
	return false
}

func (p *Processor) notifyTargetWaitingState(lctx model.Logging, log logging.Logger, e Element, missing, waiting []ElementId, inputs model.Inputs) bool {
	var keys []interface{}
	if len(inputs) > 0 {
		keys = append(keys, "found", utils.Join(utils.MapKeys(inputs)))
	}
	if len(waiting) > 0 || len(missing) > 0 {
		if len(missing) > 0 {
			keys = append(keys, "missing", utils.Join(missing))
		}
		if len(waiting) > 0 {
			keys = append(keys, "waiting", utils.Join(waiting))
		}
		log.Info("inputs not ready", keys...)
		if len(missing) > 0 {
			p.updateStatus(lctx, log, e, model.STATUS_WAITING, fmt.Sprintf("unresolved dependencies %s", utils.Join(missing)), e.GetLock())
		} else {
			p.updateStatus(lctx, log, e, model.STATUS_PENDING, fmt.Sprintf("waiting for %s", utils.Join(waiting)), e.GetLock())
		}
		return true
	}
	log.Info("inputs according to target state ready", keys...)
	return false
}

func (p *Processor) hardenTargetState(lctx model.Logging, log logging.Logger, e Element) error {
	// determine potential external objects
	exttypes := p.processingModel.MetaModel().GetTriggeringTypesForInternalType(e.Id().GetType())

	if e.GetObject().GetTargetState(e.GetPhase()) == nil {
		log.Info("target state for internal object of {{element}} already set for actual phase")
	} else {
		extstate := model.ExternalStates{}
		if len(exttypes) > 0 {
			log.Info("hardening state for external object types {{trigger-types}}", "trigger-types", exttypes)
			for _, t := range exttypes {
				id := NewObjectId(t, e.GetNamespace(), e.GetName())
				log := log.WithValues("extid", id)
				_o, err := p.processingModel.ObjectBase().GetObject(database.NewObjectId(id.GetType(), id.GetNamespace(), id.GetName()))
				if err != nil {
					if !errors.Is(err, database.ErrNotExist) {
						log.Error("cannot get external object {{extid}}", "error", err)
						return err
					}
					log.Info("external object {{extid}} not found -> state not transferred")
					continue
				}
				o := _o.(model.ExternalObject)
				state := o.GetState()
				v := state.GetVersion()
				err = o.UpdateStatus(lctx, p.processingModel.ObjectBase(), e.Id(), model.StatusUpdate{
					RunId:           utils.Pointer(e.GetLock()),
					DetectedVersion: &v,
					ObservedVersion: nil,
					Status:          utils.Pointer(model.STATUS_PREPARING),
					Message:         utils.Pointer("preparing target state"),
					ResultState:     nil,
				})
				if err != nil {
					log.Error("cannot update status for external object {{extid}}", "error", err)
					return err
				}
				extstate[id.GetType()] = state
			}
			err := e.GetObject().SetExternalState(lctx, p.processingModel.ObjectBase(), e.GetPhase(), extstate)
			if err != nil {
				log.Error("cannot update external state for internal object from {{extid}}", "error", err)
				return err
			}
			for t, s := range extstate {
				log.Info("internal object hardened state for phase {{phase}} from type {{type}} to {{version}}", "phase", e.GetPhase(), "type", t, "version", s.GetVersion())
			}
		}
	}
	return nil
}

func (p *Processor) lockGraph(lctx model.Logging, log logging.Logger, elem _Element) (*RunId, error) {
	id := NewRunId()
	ns := p.getNamespace(elem.GetNamespace())

	if !ns.lock.TryLock() {
		return nil, nil
	}
	defer ns.lock.Unlock()

	log = log.WithValues("runid", id)
	ok, err := ns.namespace.TryLock(p.processingModel.ObjectBase(), id)
	if err != nil {
		log.Info("locking namespace {{namespace}} for new runid {{runid}} failed", "error", err)
		return nil, err
	}
	if !ok {
		log.Info("cannot lock namespace {{namespace}} for already locked for {{current}}", "current", ns.namespace.GetLock())
		return nil, nil
	}
	log.Info("namespace {{namespace}} locked for new runid {{runid}}")
	defer func() {
		err := ns.clearLocks(lctx, log, p)
		if err != nil {
			log.Error("cannot clear namespace lock for {{namespace}} -> requeue", "error", err)
			p.EnqueueNamespace(ns.GetNamespaceName())
		}
	}()

	elems := map[ElementId]_Element{}
	ok, err = p._tryLockGraph(log, ns, elem, elems)
	if !ok || err != nil {
		return nil, err
	}
	ok, err = p._lockGraph(log, ns, elems, id)
	if !ok || err != nil {
		return nil, err
	}
	return &id, nil
}

func (p *Processor) _tryLockGraph(log logging.Logger, ni *namespaceInfo, elem _Element, elems map[ElementId]_Element) (bool, error) {
	if elems[elem.Id()] == nil {
		cur := elem.GetLock()
		if cur != "" {
			return false, nil
		}
		elems[elem.Id()] = elem

		for _, d := range ni.getChildren(elem.Id()) {
			ok, err := p._tryLockGraph(log, ni, d.(_Element), elems)
			if !ok || err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (p *Processor) _lockGraph(log logging.Logger, ns *namespaceInfo, elems map[ElementId]_Element, id RunId) (bool, error) {
	var ok bool
	var err error

	ns.pendingElements = map[ElementId]_Element{}

	log.Debug("found {{amount}} elements in graph", "amount", len(elems))
	for _, elem := range elems {
		log.Debug("locking {{nestedelem}}", "nestedelem", elem.Id())
		ok, err = elem.TryLock(p.processingModel.ObjectBase(), id)
		if !ok || err != nil {
			log.Debug("locking failed for {{nestedelem}}", "nestedelem", elem.Id(), "error", err)
			return false, err
		}
		ns.pendingElements[elem.Id()] = elem
		p.pending.Add(1)

	}
	ns.pendingElements = nil
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) checkReady(ni *namespaceInfo, links []ElementId) ([]ElementId, []ElementId, model.Inputs) {
	var missing []ElementId
	var waiting []ElementId
	inputs := model.Inputs{}

	ni.lock.Lock()
	defer ni.lock.Unlock()

	for _, l := range links {
		t := ni.elements[l]
		if t == nil {
			missing = append(missing, l)
		} else {
			if t.GetLock() == "" && t.GetCurrentState().GetOutputVersion() != "" {
				inputs[l] = t.GetCurrentState().GetOutput()
			} else {
				waiting = append(waiting, l)
			}
		}
	}
	return missing, waiting, inputs
}
