package processor

import (
	"errors"
	"fmt"
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processElement(lctx model.Logging, cmd string, id ElementId) pool.Status {
	defer p.events.TriggerElementHandled(id)

	nctx := lctx.WithValues("namespace", id.GetNamespace(), "element", id).WithName(id.String())
	log := nctx.Logger()
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
	if cmd == CMD_EXT {
		if runid == "" || p.isReTriggerable(elem) {
			if runid == "" {
				log.Debug("initial external change for {{element}}")
			} else {
				log.Debug("retriggering external change for {{element}}")
			}
			return p.handleExternalChange(nctx, elem)
		}
		log.Debug("skip external object trigger for {{element}}")
		return pool.StatusCompleted()
	}
	lctx = lctx.WithValues("namespace", id.GetNamespace(), "element", id, "runid", runid).WithName(string(runid)).WithName(elem.Id().String())
	return p.handleRun(lctx, elem)
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
	log.Info("processing external element trigger for {{element}} with status {{status}}", "status", e.GetStatus())

	types := p.processingModel.MetaModel().GetTriggeringTypesForElementType(e.Id().TypeId())
	if len(types) == 0 {
		log.Info("no triggering types for {{element}}")
		return pool.StatusCompleted()
	}

	if !isExtTriggerable(e) {
		if !p.isReTriggerable(e, types...) {
			log.Info("state for element in status {{status}} is already assigned", "status", e.GetStatus())
			return pool.StatusCompleted()
		}
		log.Info("state for element in status {{status}} is already assigned but retriggerable", "status", e.GetStatus())
	}

	log.Info("checking state of external objects for element {{element}}", "exttypes", types)
	var changed []string
	cur := e.GetCurrentState().GetObservedVersion()
	for _, t := range types {
		id := database.NewObjectId(t, e.GetNamespace(), e.GetName())
		log := log.WithValues("extid", id)
		_o, err := p.processingModel.ObjectBase().GetObject(id)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				log.LogError(err, "cannot get external object {{extid}}")
				return pool.StatusCompleted(fmt.Errorf("cannot get external object %s: %w", id, err))
			}
			log.Info("external object {{extid}} not found -> ignore state")
			continue
		}

		o := _o.(model.ExternalObject)
		// give the internal object the chance to modify the actual state
		ov := o.GetState().GetVersion()
		v := e.GetExternalState(o).GetVersion()
		if ov != v {
			log.Debug("state of external object {{extid}} adjusted from {{objectversion}} to {{version}}", "objectversion", ov, "version", v)
		}
		if v == cur {
			log.Info("state of external object {{extid}} not changed ({{version}})", "version", v)
		} else {
			changed = append(changed, t)
			log.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
		}
	}
	if len(changed) == 0 {
		log.Info("no external object state change found for {{element}}")
		return pool.StatusCompleted()
	}

	if p.isReTriggerable(e, changed...) {
		log.Info("retrigger state change of external object for {{element}}", "exttypes", changed)
		p.Enqueue(CMD_ELEM, e)
		return pool.StatusCompleted()
	}

	log.Info("trying to initiate new run for {{element}}")
	rid, err := p.lockGraph(lctx, log, e)
	if err == nil {
		if rid != nil {
			log.Info("starting run {{runid}}", "runid", *rid)
			p.Enqueue(CMD_ELEM, e)
		} else {
			err = fmt.Errorf("delay initiation of new run")
		}
	}
	return pool.StatusCompleted(err)
}

func (p *Processor) isReTriggerable(e _Element, ext ...string) bool {
	if e.GetLock() != "" {
		if len(ext) == 0 {
			ext = p.processingModel.MetaModel().GetTriggeringTypesForElementType(e.Id().TypeId())
		}
		for _, t := range ext {
			if p.processingModel.MetaModel().IsForeignControlled(t) {
				if isProcessable(e) {
					return true
				}
			}
		}
	}
	return false
}

func (p *Processor) handleRun(lctx model.Logging, e _Element) pool.Status {
	log := lctx.Logger()
	ni := p.getNamespace(e.GetNamespace())

	var missing, waiting []ElementId
	var inputs model.Inputs

	log = log.WithValues("status", e.GetStatus())
	log.Info("processing element {{element}} with status {{status}}")

	var links []ElementId

	if isExtTriggerable(e) || p.isReTriggerable(e) {
		// wait for inputs to become ready

		if e.GetProcessingState() == nil {
			log.Info("checking current links")
			links = e.GetCurrentState().GetLinks()

			for _, l := range links {
				if !p.processingModel.MetaModel().HasDependency(e.Id().TypeId(), l.TypeId()) {
					return p.fail(lctx, log, ni, e, fmt.Errorf("invalid dependency from %s to %s", e.Id().TypeId(), l.TypeId()))
				}
			}

			// first, check current state
			missing, waiting, inputs = p.checkReady(log, ni, "current", links)
			if ok := p.notifyCurrentWaitingState(lctx, log, e, missing, waiting, inputs); ok {
				// return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
				return pool.StatusCompleted() // TODO: require rate limiting??
			}
		}

		if e.GetProcessingState() == nil || p.isReTriggerable(e) {
			if p.isReTriggerable(e) {
				log.Info("update target state")
			} else {
				log.Info("gather target state")
			}
			// second, assign target state by transferring the current external state to the internal object
			s, err := p.assignTargetState(lctx, log, e)
			switch s {
			case model.ACCEPT_OK:
				if err != nil {
					log.Error("cannot update external state for internal object", "error", err)
				}
			case model.ACCEPT_REJECTED:
				log.Error("external state for internal object from parent rejected -> block element", "error", err)
				return p.block(lctx, log, ni, e, err.Error())

			case model.ACCEPT_INVALID:
				log.Error("external state for internal object invalid -> fail element", "error", err)
				return p.fail(lctx, log, ni, e, err)
			}
			if err != nil {
				return pool.StatusCompleted(err)
			}

			// checking target dependencies after fixing the target state
			log.Info("checking target links and get actual inputs")
			links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
			for _, l := range links {
				if !p.processingModel.MetaModel().HasDependency(e.Id().TypeId(), l.TypeId()) {
					return p.fail(lctx, log, ni, e, fmt.Errorf("invalid dependency from %s to %s", e.Id().TypeId(), l.TypeId()))
				}
			}
		} else {
			log.Info("continue interrupted processing")
			links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
		}

		missing, waiting, inputs = p.checkReady(log, ni, "target", links)
		ok, blocked, err := p.notifyTargetWaitingState(lctx, log, e, missing, waiting, inputs)
		if err != nil {
			return pool.StatusCompleted(fmt.Errorf("notifying blocked status failed: %w", err))
		}
		if blocked {
			p.pending.Add(-1)
			p.triggerChildren(log, ni, e, true)
			log.Info("unresolvable dependencies {{waiting}}", "waiting", utils.Join(waiting))
			return pool.StatusFailed(fmt.Errorf("unresolvable dependencies %s", utils.Join(waiting)))
		}
		if ok {
			return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))

			log.Info("missing dependencies {{waiting}}", "waiting", utils.Join(waiting))
			return pool.StatusCompleted(nil) // TODO: rate limiting required?
		}

		if e.GetProcessingState() == nil {
			// mark element to be ready by setting the element's target state to the target state of the internal
			// object for the actual phase
			e.SetProcessingState(NewTargetState(e))
		}

		// check effective version for required phase processing.
		target := e.GetObject().GetTargetState(e.GetPhase())

		indiff := diff(log, "input version", e.GetCurrentState().GetInputVersion(), target.GetInputVersion(inputs))
		obdiff := diff(log, "object version", e.GetCurrentState().GetObjectVersion(), target.GetObjectVersion())
		if !indiff && !obdiff {
			log.Info("effective version unchanged -> skip processing of phase")
			err := p.notifyCompletedState(lctx, log, ni, e, "no processing required", nil, nil)
			if err == nil {
				_, err = e.SetStatus(p.processingModel.ObjectBase(), model.STATUS_COMPLETED)
				if err == nil {
					p.pending.Add(-1)
					p.triggerChildren(log, ni, e, true)
				}
			}
			return pool.StatusCompleted(err)
		}

		upstate := func(log logging.Logger, o model.ExternalObject) error {
			return o.UpdateStatus(lctx, p.processingModel.ObjectBase(), e.Id(), model.StatusUpdate{
				Status:  utils.Pointer(model.STATUS_PROCESSING),
				Message: utils.Pointer(fmt.Sprintf("processing phase %s", e.GetPhase())),
			})
		}

		log.Info("update processing status of external objects")
		err = p.forExtObjects(log, e, upstate)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		err = p.setStatus(log, e, model.STATUS_PROCESSING)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	} else {
		links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
		missing, waiting, inputs = p.checkReady(log, ni, "target", links)
		if len(missing) > 0 || len(waiting) > 0 {
			log.Error("unexpected state of parents, should be available, but found missing {{missing}} and/or waiting {{waiting}}",
				"missing", utils.Join(missing), "waiting", utils.Join(waiting))
			return p.fail(lctx, log, ni, e, fmt.Errorf("unexpected state of parents"))
		}
	}

	if isProcessable(e) {
		// now we can process the phase
		log.Info("executing phase {{phase}} of internal object {{intid}}", "phase", e.GetPhase(), "intid", e.Id().ObjectId())
		result := e.GetObject().Process(model.Request{
			Logging:         lctx,
			Model:           p.processingModel,
			Element:         e,
			Inputs:          inputs,
			ElementAccess:   ni,
			SlaveManagement: newSlaveManagement(log, p, ni, e),
		})

		if result.Error != nil {
			if result.Status == model.STATUS_FAILED {
				// non-recoverable error, wait for new change in external object state
				log.Error("processing provides non recoverable error", "error", result.Error)
				return p.fail(lctx, log, ni, e, result.Error)
			}
			log.Error("processing provides error", "error", result.Error)
			err := p.updateStatus(lctx, log, e, result.Status, result.Error.Error())
			if err != nil {
				return pool.StatusCompleted(err)
			}
			err = p.setStatus(log, e, result.Status)
			if err != nil {
				return pool.StatusCompleted(err)
			}
			return pool.StatusCompleted(result.Error)
		} else {
			switch result.Status {
			case model.STATUS_FAILED:
				p.setStatus(log, e, model.STATUS_FAILED)
				p.pending.Add(-1)
				p.triggerChildren(log, ni, e, true)
			case model.STATUS_DELETED:
				p.internalObjectDeleted(log, ni, e)
				p.setStatus(log, e, model.STATUS_DELETED)
				p.pending.Add(-1)
				p.triggerChildren(log, ni, e, true)
			case model.STATUS_COMPLETED:
				err := p.notifyCompletedState(lctx, log, ni, e, "processing completed", result.EffectiveObjectVersion, inputs, result.ResultState, CalcEffectiveVersion(inputs, e.GetProcessingState().GetObjectVersion()))
				if err != nil {
					return pool.StatusCompleted(err)
				}
				p.setStatus(log, e, model.STATUS_COMPLETED)
				p.pending.Add(-1)
				p.triggerChildren(log, ni, e, true)
			default:
				p.setStatus(log, e, result.Status)
			}
		}
	} else {
		log.Info("element with status {{status}} is not processable")
	}
	return pool.StatusCompleted()
}

func diff(log logging.Logger, kind string, old, new string) bool {
	diff := old != new
	if diff {
		log.Info(fmt.Sprintf("%s changed from {{old}} -> {{new}}", kind), "new", new, "old", old)
	} else {
		log.Info(fmt.Sprintf("%s unchanged", kind))
	}
	return diff
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
	log.Info("internal object {{element}} deleted by processing step")
	for _, ph := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		for _, c := range ni.GetChildren(NewElementIdForPhase(elem, ph)) {
			if !slices.Contains(children, c.Id()) {
				children = append(children, c.Id())
			}
		}
	}
	for _, ph := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		log.Info("- deleting phase {{phase}}", "phase", ph)
		delete(ni.elements, NewElementIdForPhase(elem, ph))
	}
	for _, c := range children {
		log.Info("  trigger dependent element {{depelem}}", "depelem", c)
		p.EnqueueKey(CMD_ELEM, c)
	}
}

func (p *Processor) notifyCompletedState(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, msg string, eff *string, inputs model.Inputs, args ...interface{}) error {
	var ci *model.CommitInfo

	result := GetResultState(args...)
	target := e.GetProcessingState()
	if result != nil {
		ci = &model.CommitInfo{
			InputVersion:  target.GetInputVersion(inputs),
			ObjectVersion: eff,
			OutputState:   result,
		}
	}
	if target != nil {
		log.Info("committing target state")
		_, err := e.Commit(lctx, p.processingModel.ObjectBase(), e.GetLock(), ci)
		if err != nil {
			log.Error("cannot unlock element {{element}}", "error", err)
			return err
		}
		e.SetProcessingState(nil)
	} else {
		log.Info("skipping commit of target state")
	}
	log.Info("completed processing of element {{element}}", "output")
	err := p.updateStatus(lctx, log, e, model.STATUS_COMPLETED, msg, append(args, RunId(""))...)
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) notifyCurrentWaitingState(lctx model.Logging, log logging.Logger, e _Element, missing, waiting []ElementId, inputs model.Inputs) bool {
	var keys []interface{}
	if len(missing) > 0 {
		keys = append(keys, "ignored missing", utils.Join(missing))
	}
	if len(inputs) > 0 {
		keys = append(keys, "found", utils.Join(utils.MapKeys(inputs)))
	}
	if len(waiting) > 0 {
		keys = append(keys, "waiting", utils.Join(waiting))
		log.Info("inputs according to current state not ready", keys...)
		return true
	}
	if len(missing) > 0 {
		log.Info("found missing dependencies {{missing}}, but other dependencies ready {{found}} -> continue with target state", keys...)
	} else {
		log.Info("inputs according to current state ready", keys...)
	}
	return false
}

func (p *Processor) notifyTargetWaitingState(lctx model.Logging, log logging.Logger, e _Element, missing, waiting []ElementId, inputs model.Inputs) (bool, bool, error) {
	var keys []interface{}
	if len(inputs) > 0 {
		keys = append(keys, "found", utils.Join(utils.MapKeys(inputs)))
	}
	if len(missing) > 0 {
		keys = append(keys, "missing", utils.Join(missing))
	}
	if len(waiting) > 0 {
		keys = append(keys, "waiting", utils.Join(waiting))
	}
	if len(missing) > 0 {
		log.Info("inputs according to target state not ready", keys...)
		return true, true, p.blocked(lctx, log, e, fmt.Sprintf("unresolved dependencies %s", utils.Join(missing)))
	}
	if len(waiting) > 0 {
		log.Info("inputs according to target state not ready", keys...)
		return true, false, nil
	}
	log.Info("inputs according to target state ready", keys...)
	return false, false, nil
}

func (p *Processor) block(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, msg string) pool.Status {
	err := p.blocked(lctx, log, e, msg)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	p.pending.Add(-1)
	p.triggerChildren(log, ni, e, true)
	return pool.StatusCompleted()
}

func (p *Processor) blocked(lctx model.Logging, log logging.Logger, e _Element, msg string) error {
	err := p.updateStatus(lctx, log, e, model.STATUS_BLOCKED, msg, e.GetLock())
	if err == nil {
		_, err = e.Rollback(lctx, p.processingModel.ObjectBase(), e.GetLock(), true)
	}
	if err == nil {
		err = p.setStatus(log, e, model.STATUS_BLOCKED)
	}
	return err
}

func (p *Processor) fail(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, fail error) pool.Status {
	err := p.failed(lctx, log, e, fail.Error())
	if err != nil {
		return pool.StatusCompleted(err)
	}
	p.pending.Add(-1)
	p.triggerChildren(log, ni, e, true)
	return pool.StatusFailed(fail)

}

func (p *Processor) failed(lctx model.Logging, log logging.Logger, e _Element, msg string) error {
	err := p.updateStatus(lctx, log, e, model.STATUS_FAILED, msg, e.GetLock())
	if err == nil {
		_, err = e.Rollback(lctx, p.processingModel.ObjectBase(), e.GetLock(), true)
	}
	if err == nil {
		err = p.setStatus(log, e, model.STATUS_FAILED)
	}
	return err
}

func (p *Processor) assignTargetState(lctx model.Logging, log logging.Logger, e _Element) (model.AcceptStatus, error) {
	// determine potential external objects
	if e.GetObject().GetTargetState(e.GetPhase()) == nil {
		log.Info("target state for internal object of {{element}} already set for actual phase -> update state")
	}

	extstate := model.ExternalStates{}

	mod := func(log logging.Logger, o model.ExternalObject) error {
		if isProcessable(e) && !p.isReTriggerable(e, o.GetType()) {
			return nil
		}
		state := e.GetExternalState(o)
		v := state.GetVersion()
		log.Trace("  found effective external state from {{extid}} for phase {{phase}}: {{state}}",
			"phase", e.GetPhase(), "state", DescribeObject(state))
		err := o.UpdateStatus(lctx, p.processingModel.ObjectBase(), e.Id(), model.StatusUpdate{
			RunId:           utils.Pointer(e.GetLock()),
			DetectedVersion: &v,
			ObservedVersion: nil,
			Status:          utils.Pointer(model.STATUS_PREPARING),
			Message:         utils.Pointer("preparing target state"),
			ResultState:     nil,
		})
		if err != nil {
			p.setStatus(log, e, model.STATUS_PREPARING)
			log.Error("cannot update status for external object {{extid}}", "error", err)
			return err
		}
		extstate[o.GetType()] = state
		return nil
	}

	log.Info("gathering state for external object types")
	err := p.forExtObjects(log, e, mod)
	if err != nil {
		return model.ACCEPT_OK, err
	}

	if len(extstate) == 0 {
		log.Info("no external object states found for {{element}}  -> propagate empty state")
	} else {
		log.Info("assigning external state for processing {{element}}", "exttypes", utils.MapKeys(extstate))
	}
	s, err := e.GetObject().AcceptExternalState(lctx, p.processingModel.ObjectBase(), e.GetPhase(), extstate)
	if s != model.ACCEPT_OK || err != nil {
		return s, err
	}
	for t, st := range extstate {
		log.Info("- assigned state for phase {{phase}} from type {{type}} to {{version}}", "phase", e.GetPhase(), "type", t, "version", st.GetVersion())
	}
	return s, nil
}

func (p *Processor) lockGraph(lctx model.Logging, log logging.Logger, elem _Element) (*RunId, error) {
	id := NewRunId()
	ni := p.getNamespace(elem.GetNamespace())

	if !ni.lock.TryLock() {
		return nil, nil
	}
	defer ni.lock.Unlock()

	log = log.WithValues("runid", id)
	ok, err := ni.tryLock(p, id)
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
		err := ni.clearLocks(lctx, log, p)
		if err != nil {
			log.Error("cannot clear namespace lock for {{namespace}} -> requeue", "error", err)
			p.EnqueueNamespace(ni.GetNamespaceName())
		} else {

		}
	}()

	elems := map[ElementId]_Element{}
	ok, err = p._tryLockGraph(log, ni, elem, elems)
	if !ok || err != nil {
		return nil, err
	}
	ok, err = p._lockGraph(log, ni, elems, id)
	if !ok || err != nil {
		return nil, err
	}
	return &id, nil
}

func (p *Processor) _tryLockGraph(log logging.Logger, ni *namespaceInfo, elem _Element, elems map[ElementId]_Element) (bool, error) {
	if elems[elem.Id()] == nil {
		cur := elem.GetLock()
		if cur != "" {
			log.Info("element {{candidate}} already locked for {{lock}}", "candidate", elem.Id(), "lock", cur)
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
		p.events.TriggerElementHandled(elem.Id())
		p.pending.Add(1)

	}
	ns.pendingElements = nil
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) checkReady(log logging.Logger, ni *namespaceInfo, kind string, links []ElementId) ([]ElementId, []ElementId, model.Inputs) {
	var missing []ElementId
	var waiting []ElementId
	inputs := model.Inputs{}

	log.Debug(fmt.Sprintf("evaluating %s links {{links}}", kind), "links", links)
	ni.lock.Lock()
	defer ni.lock.Unlock()

	for _, l := range links {
		t := ni.elements[l]
		if t == nil {
			log.Debug(" - {{link}} not found", "link", l)
			missing = append(missing, l)
			continue
		}
		if t.GetLock() == "" && t.GetCurrentState().GetOutputVersion() != "" {
			inputs[l] = t.GetCurrentState().GetOutput()
			log.Debug(" - {{link}} is unlocked and has output state", "link", l)
			continue
		}
		if t.GetLock() != "" {
			log.Debug(" - {{link}} still locked", "link", l)
			waiting = append(waiting, l)
			continue
		}
		if t.GetCurrentState().GetOutputVersion() == "" {
			log.Debug(" - {{link}} has no output version", "link", l)
			missing = append(missing, l)
			continue
		}
		waiting = append(waiting, l)
	}
	return missing, waiting, inputs
}
