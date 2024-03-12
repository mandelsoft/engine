package processor

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processElement(lctx model.Logging, cmd string, id ElementId) pool.Status {
	if p.delay > 0 {
		time.Sleep(p.delay)
	}

	nctx := lctx.WithValues("namespace", id.GetNamespace(), "element", id).WithName(id.String())
	log := nctx.Logger()
	elem := p.processingModel._GetElement(id)

	if elem != nil && elem.GetStatus() == model.STATUS_DELETED {
		log.Debug("skip deleted element {{element}}")
		return pool.StatusCompleted()
	}

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
		if runid == "" || p.isReTriggerable(elem) || elem.IsMarkedForDeletion() {
			if runid == "" {
				log.Debug("new external change for {{element}} (deletion requested: {{marked}})", "marked", elem.IsMarkedForDeletion())
			} else {
				log.Debug("retriggering external change for {{element}} (deletion requested: {{marked}})", "marked", elem.IsMarkedForDeletion())
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
	log.Info("processing unknown element {{element}}")

	_i, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return nil, pool.StatusCompleted(err)
		}
		log.Info("internal object not found for {{element}}")

		_, ext, err := p.getTriggeringExternalObject(id)
		if err != nil {
			return nil, pool.StatusCompleted(err)
		}

		if ext == nil {
			log.Info("no triggering object found -> obsolete event -> don't create new element")
			return nil, pool.StatusCompleted()
		}
		if p.isDeleting(ext) {
			log.Info("external object is deleting -> don't create new element")
			return nil, pool.StatusCompleted()
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

	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(e.Id().TypeId())
	if trigger == nil {
		log.Info("no triggering types for {{element}}")
		return pool.StatusCompleted()
	}

	if !isExtTriggerable(e) {
		if !p.isReTriggerable(e, *trigger) {
			if !e.IsMarkedForDeletion() {
				log.Info("state for element in status {{status}} is already assigned", "status", e.GetStatus())
				return pool.StatusCompleted()
			}
		}
		log.Info("state for element in status {{status}} is already assigned but retriggerable", "status", e.GetStatus())
	}

	log.Info("checking state of external objects for element {{element}}", "exttypes", trigger)
	var changed *string
	deleting := false
	cur := e.GetCurrentState().GetObservedState().GetObjectVersion()

	id := database.NewObjectId(*trigger, e.GetNamespace(), e.GetName())
	log = log.WithValues("extid", id)
	_o, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			log.LogError(err, "cannot get external object {{extid}}")
			return pool.StatusCompleted(fmt.Errorf("cannot get external object %s: %w", id, err))
		}
		log.Info("external object {{extid}} not found -> ignore state")
		return pool.StatusCompleted()
	}

	o := _o.(model.ExternalObject)
	if o.IsDeleting() {
		log.Info("external object {{extid}} requests deletion")
		deleting = true
	}

	// give the internal object the chance to modify the actual external state
	ov := o.GetState().GetVersion()
	v := e.GetExternalState(o).GetVersion()
	if ov != v {
		log.Debug("state of external object {{extid}} adjusted from {{objectversion}} to {{version}}", "objectversion", ov, "version", v)
	}
	if v == cur {
		log.Info("state of external object {{extid}} not changed ({{version}})", "version", v)
	} else {
		changed = trigger
		log.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
	}

	if changed == nil && !deleting {
		log.Info("no external object state change found for {{element}}")
		return pool.StatusCompleted()
	}

	var leafs []Phase
	var phases []Phase
	if deleting {
		var ok bool
		var err error
		log.Info("element {{element}} should be deleted")
		ok, phases, leafs, err = e.MarkForDeletion(p.processingModel)
		if err != nil {
			log.LogError(err, "cannot mark all dependent phases ({phases}}) of {{element}} as deleting", "phases", phases)
			return pool.StatusCompleted(err)
		}
		if ok {
			log.Info("marked dependent phases ({{phases}}) of {{element}} as deleting", "phases", phases)
		}
	}

	if changed != nil && p.isReTriggerable(e, *changed) {
		log.Info("retrigger state change of external object {{extid}} for {{element}}")
		p.Enqueue(CMD_ELEM, e)
		return pool.StatusCompleted()
	}

	if changed != nil {
		return p.initiateNewRun(lctx, log, e)
	}

	log.Info("triggering leaf phases {{phases}} for deletion", "phases", leafs)
	for _, phase := range leafs {
		id := NewElementIdForPhase(e, phase)
		log.Debug(" - triggering {{leaf}}", "leaf", id)
		p.EnqueueKey(CMD_ELEM, id)
	}
	return pool.StatusCompleted()
}

func (p *Processor) initiateNewRun(lctx model.Logging, log logging.Logger, e _Element) pool.Status {
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
		if e.IsMarkedForDeletion() {
			return true
		}
		var ttyp *string
		if len(ext) == 0 {
			ttyp = p.processingModel.MetaModel().GetTriggerTypeForElementType(e.Id().TypeId())
		} else {
			ttyp = &ext[0]
		}
		if ttyp != nil {
			if p.processingModel.MetaModel().IsForeignControlled(*ttyp) {
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

	var ready *ReadyState

	log = log.WithValues("status", e.GetStatus())
	mode := "process"
	if e.IsMarkedForDeletion() {
		mode = "delete"
	}
	log.Info("processing element ({{mode}}) {{element}} with status {{status}} (finalizers {{finalizers}})", "finalizers", e.GetObject().GetFinalizers(), "mode", mode)

	var links []ElementId
	var formalVersion string

	curlinks := e.GetCurrentState().GetLinks()
	deletion := false
	if e.IsMarkedForDeletion() {
		err := e.GetObject().PrepareDeletion(lctx, newSlaveManagement(log, p, ni, e), e.GetPhase())
		if err != nil {
			return pool.StatusCompleted(err)
		}

		children := ni.GetChildren(e.Id())
		if len(children) == 0 {
			log.Info("element {{element}} is deleting and no children found -> initiate deletion")
			links = curlinks
			deletion = true
		} else {
			var list []ElementId
			for _, c := range children {
				list = append(list, c.Id())
			}
			log.Info("element {{element}} is deleting but still has children ({{children}}) found -> normal processing", "children", list)
		}
	}
	if !deletion && e.GetLock() == "" {
		if e.GetStatus() == model.STATUS_BLOCKED {
			log.Info("checking ready condition for blocked element {{element}}")
			ready := p.isReady(log, ni, e)
			if ready.ReadyForTrigger() {
				log.Info("all links ready for consumption -> initiate new run")
				return p.initiateNewRun(lctx, log, e)
			}
			log.Info("found still missing elements for {{element}} ({{missing}}) -> keep state blocked", "missing", utils.Join(ready.BlockingElements(), ","))
		} else {
			log.Info("no active run for {{element}} -> skip processing")
		}
		return pool.StatusCompleted()
	}

	if !deletion {
		if isExtTriggerable(e) || p.isReTriggerable(e) {
			// wait for inputs to become ready

			if e.GetProcessingState() == nil {
				log.Info("checking current links")
				links = e.GetCurrentState().GetLinks()

				if err := p.verifyLinks(e, links...); err != nil {
					return p.fail(lctx, log, ni, e, true, err)
				}

				// first, check current state
				ready = p.checkReady(log, ni, "current", e.GetLock(), links)
				if ok := p.notifyCurrentWaitingState(lctx, log, e, ready); ok {
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

				case model.ACCEPT_INVALID:
					log.Error("external state for internal object invalid -> fail element", "error", err)
					return p.fail(lctx, log, ni, e, true, err)
				}
				if err != nil {
					return pool.StatusCompleted(err)
				}

				// checking target dependencies after fixing the target state
				log.Info("checking target links and get actual inputs")
				links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
				if err := p.verifyLinks(e, links...); err != nil {
					return p.fail(lctx, log, ni, e, true, err)
				}
			} else {
				log.Info("continue interrupted processing")
				links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
			}

			ready = p.checkReady(log, ni, "target", e.GetLock(), links)
			ok, blocked, err := p.notifyTargetWaitingState(lctx, log, e, ready)
			if err != nil {
				return pool.StatusCompleted(fmt.Errorf("notifying blocked status failed: %w", err))
			}
			if blocked {
				p.pending.Add(-1)
				p.triggerChildren(log, ni, e, true)
				log.Info("unresolvable dependencies {{waiting}}", "waiting", utils.Join(ready.Waiting))
				return pool.StatusFailed(fmt.Errorf("unresolvable dependencies %s", utils.Join(ready.Waiting)))
			}
			if ok {
				return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
				// TODO: trigger waiting by completed element of foreign run !!!!!!!!!!!!!
				log.Info("missing dependencies {{waiting}}", "waiting", utils.Join(ready.Waiting))
				return pool.StatusCompleted(nil) // TODO: rate limiting required?
			}

			if e.GetProcessingState() == nil {
				// mark element to be ready by setting the element's target state to the target state of the internal
				// object for the actual phase
				e.SetProcessingState(NewTargetState(e))
			}

			// check effective version for required phase processing.
			target := e.GetObject().GetTargetState(e.GetPhase())

			indiff := diff(log, "input version", e.GetCurrentState().GetInputVersion(), target.GetInputVersion(ready.Inputs))
			obdiff := diff(log, "object version", e.GetCurrentState().GetObjectVersion(), target.GetObjectVersion())
			if !indiff && !obdiff {
				log.Info("effective version unchanged -> skip processing of phase")
				err := p.notifyCompletedState(lctx, log, ni, e, "no processing required", nil, ready.Inputs)
				if err == nil {
					_, err = e.SetStatus(p.processingModel.ObjectBase(), model.STATUS_COMPLETED)
					if err == nil {
						p.pending.Add(-1)
						p.triggerChildren(log, ni, e, true)
					}
				}
				return pool.StatusCompleted(err)
			}

			formalVersion = p.formalVersion(e, ready.Inputs)

			upstate := func(log logging.Logger, o model.ExternalObject) error {
				return o.UpdateStatus(lctx, p.processingModel.ObjectBase(), e.Id(), model.StatusUpdate{
					Status:        utils.Pointer(model.STATUS_PROCESSING),
					FormalVersion: utils.Pointer(formalVersion),
					Message:       utils.Pointer(fmt.Sprintf("processing phase %s", e.GetPhase())),
				})
			}

			log.Info("update processing status of external objects")
			err = p.forExtObjects(log, e, upstate, UpdateObjects)
			if err != nil {
				return pool.StatusCompleted(err)
			}

			err = p.setStatus(log, e, model.STATUS_PROCESSING)
			if err != nil {
				return pool.StatusCompleted(err)
			}
		} else {
			links = e.GetObject().GetTargetState(e.GetPhase()).GetLinks()
			ready = p.checkReady(log, ni, "target", e.GetLock(), links)
			if !ready.Ready() {
				log.Error("unexpected state of parents, should be available, but found missing {{missing}} and/or waiting {{waiting}}",
					"missing", utils.Join(ready.BlockingElements()), "waiting", utils.Join(ready.Waiting))
				return p.fail(lctx, log, ni, e, false, fmt.Errorf("unexpected state of parents"))
			}
			formalVersion = p.formalVersion(e, ready.Inputs)
		}
	} else {
		err := p.setStatus(log, e, model.STATUS_DELETING)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}

	if !deletion {
		log.Debug("working on formal target version {{formal}}", "formal", formalVersion)
	}

	if isProcessable(e) {
		// now we can process the phase
		log.Info("executing phase {{phase}} of internal object {{intid}} (deletion {{deletion}})", "phase", e.GetPhase(), "intid", e.Id().ObjectId(), "deletion", deletion)
		request := model.Request{
			Logging:         lctx,
			Model:           p.processingModel,
			Element:         e,
			Delete:          deletion,
			FormalVersion:   formalVersion,
			ElementAccess:   ni,
			SlaveManagement: newSlaveManagement(log, p, ni, e),
		}
		if ready != nil {
			request.Inputs = ready.Inputs
		}
		result := e.GetObject().Process(request)

		if result.Error != nil {
			if result.Status == model.STATUS_FAILED || result.Status == model.STATUS_INVALID {
				// non-recoverable error, wait for new change in external object state
				log.Error("processing provides non recoverable error", "error", result.Error)
				return p.fail(lctx, log, ni, e, result.Status == model.STATUS_INVALID, result.Error, formalVersion)
			}
			log.Error("processing provides error", "error", result.Error)
			err := p.updateStatus(lctx, log, e, result.Status, result.Error.Error(), FormalVersion(formalVersion))
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
				err := p.phaseDeleted(lctx, log, ni, e)
				if err != nil {
					return pool.StatusCompleted(err)
				}
				p.events.TriggerStatusEvent(log, e)
				p.pending.Add(-1)
				p.triggerLinks(log, "parent", links...)
				p.triggerChildren(log, ni, e, true)
			case model.STATUS_COMPLETED:
				err := p.notifyCompletedState(lctx, log, ni, e, "processing completed", result.EffectiveObjectVersion, ready.Inputs,
					result.ResultState,
					CalcEffectiveVersion(ready.Inputs, e.GetProcessingState().GetObjectVersion()))
				if err != nil {
					return pool.StatusCompleted(err)
				}
				p.setStatus(log, e, model.STATUS_COMPLETED)
				p.pending.Add(-1)
				p.triggerChildren(log, ni, e, true)

				vanished := false
				for _, l := range curlinks {
					if !slices.Contains(links, l) {
						if !vanished {
							log.Info("triggering vanished links")
							vanished = true
						}
						log.Info(" - trigger vanished link {{link}}", "link", l)
					}
					p.EnqueueKey(CMD_ELEM, l)
				}
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
	tolock, _ := p.processingModel.MetaModel().GetDependentTypePhases(NewTypeId(i.GetType(), phase))
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

func (p *Processor) phaseDeleted(lctx pool.MessageContext, log logging.Logger, ni *namespaceInfo, elem _Element) error {
	log.Info("phase {{element}} deleted by processing step")
	err := p.updateStatus(lctx, log, elem, model.STATUS_DELETED, "deleted")
	if err != nil {
		return err
	}
	// delay events
	err = p.setStatus(log, elem, model.STATUS_DELETED, false)
	if err != nil {
		return err
	}

	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(elem.Id().TypeId())
	var ext []model.ExternalObject
	if trigger != nil {
		oid := model.SlaveObjectId(elem.Id(), *trigger)
		o, err := p.processingModel.ObjectBase().GetObject(oid)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				return err
			}
			log.Info(" - triggering external object {{oid}} already gone", "oid", oid)
		} else {
			ext = append(ext, o.(model.ExternalObject))
		}
	}
	if len(ext) != 0 {
		for _, o := range ext {
			log.Info(" - removing finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
			ok, err := o.RemoveFinalizer(p.processingModel.ObjectBase(), FINALIZER)
			if err != nil {
				log.LogError(err, "cannot remove finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
				return err
			}
			if !ok {
				log.Info("   no finalizer for triggering external object {{oid}} to remove", "oid", NewObjectIdFor(o))
			}
		}
	}

	var phases_del []Phase
	var phases_val []Phase
	for _, phase := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		eid := NewElementIdForPhase(elem, phase)
		e := ni.GetElement(eid)
		if e != nil {
			if e.GetStatus() == model.STATUS_DELETED {
				phases_del = append(phases_del, e.GetPhase())
			} else {
				phases_val = append(phases_val, e.GetPhase())
			}
		}
	}

	if len(phases_val) > 0 {
		log.Info("found still undeleted phases {{phases}} for internal element", "phases", phases_val)
		log.Info("internal object not deleted")
		return nil
	}
	log.Info("all phases ({{phases}}) in status deleted", "phases", phases_del)

	log.Info("removing finalizer for internal object {{oid}}", "oid", NewObjectIdFor(elem.GetObject()))
	_, err = elem.GetObject().RemoveFinalizer(p.processingModel.ObjectBase(), FINALIZER)
	if err != nil {
		log.LogError(err, "cannot remove finalizer for internal object {{oid}}", "oid", NewObjectIdFor(elem.GetObject()))
		return err
	}
	log.Info("deleting internal object {{oid}}", "oid", NewObjectIdFor(elem.GetObject()))
	_, err = p.processingModel.ObjectBase().DeleteObject(elem.GetObject())
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			log.LogError(err, "cannot delete internal object {{oid}}", "oid", NewObjectIdFor(elem.GetObject()))
			return err
		}
		return err
	}

	log.Info("removing element {{element}} from processing model")
	var children []ElementId
	for _, ph := range p.processingModel.MetaModel().Phases(elem.GetType()) {
		for _, c := range ni.GetChildren(NewElementIdForPhase(elem, ph)) {
			if !slices.Contains(children, c.Id()) {
				children = append(children, c.Id())
			}
		}
	}

	ni.RemoveInternal(log, p.processingModel, NewObjectIdFor(elem))

	for _, c := range children {
		log.Info("  trigger dependent element {{depelem}}", "depelem", c)
		p.EnqueueKey(CMD_ELEM, c)
	}
	return nil
}

func (p *Processor) notifyCompletedState(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, msg string, eff *string, inputs model.Inputs, args ...interface{}) error {
	var ci *model.CommitInfo

	formal := p.formalVersion(e, inputs)
	result := GetResultState(args...)
	target := e.GetProcessingState()
	if result != nil {
		ci = &model.CommitInfo{
			FormalVersion: formal,
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
	err := p.updateStatus(lctx, log, e, model.STATUS_COMPLETED, msg, append(args, RunId(""), FormalVersion(formal))...)
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) notifyCurrentWaitingState(lctx model.Logging, log logging.Logger, e _Element, ready *ReadyState) bool {
	var keys []interface{}

	keys = append(keys, "found", utils.Join(utils.MapKeys(ready.Inputs)))
	if !ready.ReadyForTrigger() {
		keys = append(keys, "missing", utils.Join(ready.BlockingElements()))
	}
	if len(ready.Waiting) > 0 {
		keys = append(keys, "waiting", utils.Join(ready.Waiting))
		log.Info("inputs according to current state not ready", keys...)
		return true
	}
	if !ready.ReadyForTrigger() {
		log.Info("found missing dependencies {{missing}}, but other dependencies ready {{found}} -> continue with target state", keys...)
	} else {
		log.Info("inputs according to current state ready", keys...)
	}
	return false
}

func (p *Processor) notifyTargetWaitingState(lctx model.Logging, log logging.Logger, e _Element, ready *ReadyState) (bool, bool, error) {
	var keys []interface{}
	if len(ready.Inputs) > 0 {
		keys = append(keys, "found", utils.Join(utils.MapKeys(ready.Inputs)))
	}
	if !ready.ReadyForTrigger() {
		keys = append(keys, "missing", utils.Join(ready.BlockingElements()))
	}
	if len(ready.Waiting) > 0 {
		keys = append(keys, "waiting", utils.Join(ready.Waiting))
	}
	if !ready.ReadyForTrigger() {
		log.Info("inputs according to target state not ready", keys...)
		return true, true, p.blocked(lctx, log, e, fmt.Sprintf("unresolved dependencies %s", utils.Join(ready.BlockingElements())))
	}
	if len(ready.Waiting) > 0 {
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

func (p *Processor) fail(lctx model.Logging, log logging.Logger, ni *namespaceInfo, e _Element, invalid bool, fail error, formal ...string) pool.Status {
	err := p.failed(lctx, log, e, invalid, fail.Error(), formal...)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	p.pending.Add(-1)
	p.triggerChildren(log, ni, e, true)
	return pool.StatusFailed(fail)

}

func (p *Processor) failed(lctx model.Logging, log logging.Logger, e _Element, invalid bool, msg string, formal ...string) error {
	status := model.STATUS_FAILED
	if invalid {
		status = model.STATUS_INVALID
	}
	opts := []interface{}{
		e.GetLock(),
	}
	if len(formal) > 0 && formal[0] != "" {
		opts = append(opts, FormalVersion(formal[0]))
	}
	err := p.updateStatus(lctx, log, e, status, msg, opts...)
	if err == nil {
		_, err = e.Rollback(lctx, p.processingModel.ObjectBase(), e.GetLock(), true, formal...)
	}
	if err == nil {
		err = p.setStatus(log, e, status)
	}
	return err
}

func (p *Processor) assignTargetState(lctx model.Logging, log logging.Logger, e _Element) (model.AcceptStatus, error) {
	// determine potential external objects
	if e.GetObject().GetTargetState(e.GetPhase()) != nil {
		log.Info("target state for internal object of {{element}} already set for actual phase -> update state")
	}

	var extstate model.ExternalState

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
		extstate = state
		return nil
	}

	log.Info("gathering state for external object types")
	err := p.forExtObjects(log, e, mod, TriggeringObject)
	if err != nil {
		return model.ACCEPT_OK, err
	}

	if extstate == nil {
		log.Info("no external object states found for {{element}}  -> propagate empty state")
	} else {
		log.Info("assigning external state for processing {{element}}", "extstate", extstate)
	}
	s, err := e.GetObject().AcceptExternalState(lctx, p.processingModel.ObjectBase(), e.GetPhase(), extstate)
	if s != model.ACCEPT_OK || err != nil {
		return s, err
	}
	if extstate != nil {
		log.Info("assigned state for phase {{phase}} from type {{type}} to {{version}}",
			"phase", e.GetPhase(),
			"type", *p.processingModel.MetaModel().GetTriggerTypeForElementType(e.Id().TypeId()),
			"version", extstate.GetVersion())
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

	elems := NewOrderedElementSet()
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

func (p *Processor) _tryLockGraph(log logging.Logger, ni *namespaceInfo, elem _Element, elems OrderedElementSet) (bool, error) {
	if !elems.Has(elem.Id()) {
		cur := elem.GetLock()
		if cur != "" {
			log.Info("element {{candidate}} already locked for {{lock}}", "candidate", elem.Id(), "lock", cur)
			return false, nil
		}
		elems.Add(elem)

		for _, d := range ni.getChildren(elem.Id()) {
			ok, err := p._tryLockGraph(log, ni, d.(_Element), elems)
			if !ok || err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (p *Processor) _lockGraph(log logging.Logger, ns *namespaceInfo, elems OrderedElementSet, id RunId) (bool, error) {
	var ok bool
	var err error

	ns.pendingElements = map[ElementId]_Element{}

	log.Debug("found {{amount}} elements in graph", "amount", elems.Size())
	for _, elem := range elems.Order() {
		log.Debug("locking {{nestedelem}}", "nestedelem", elem.Id())
		ok, err = elem.TryLock(p.processingModel.ObjectBase(), id)
		if !ok || err != nil {
			log.Debug("locking failed for {{nestedelem}}", "nestedelem", elem.Id(), "error", err)
			return false, err
		}
		ns.pendingElements[elem.Id()] = elem
		// log.Debug("successfully locked {{nestedelem}}", "nestedelem", elem.Id())
		p.events.TriggerElementEvent(elem)
		p.pending.Add(1)
	}
	ns.pendingElements = nil
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) isReady(log logging.Logger, ni *namespaceInfo, e _Element) *ReadyState {
	links := e.GetCurrentState().GetObservedState().GetLinks()
	return p.checkReady(log, ni, "observed", e.GetLock(), links)
}

func (p *Processor) checkReady(log logging.Logger, ni *namespaceInfo, kind string, lock RunId, links []ElementId) *ReadyState {
	state := NewReadyState()

	log.Debug(fmt.Sprintf("evaluating %s links {{links}}", kind), "links", links)
	ni.lock.Lock()
	defer ni.lock.Unlock()

	for _, l := range links {
		t := ni.elements[l]
		if t == nil {
			log.Debug(" - {{link}} not found", "link", l)
			state.AddMissing(l)
			continue
		}
		if t.GetLock() == "" && t.GetCurrentState().GetOutputVersion() != "" {
			state.AddInput(l, t.GetCurrentState().GetOutput())
			log.Debug(" - {{link}} is unlocked and has output state", "link", l)
			continue
		}
		if t.GetLock() != "" {
			if lock != "" && t.GetLock() != lock {
				log.Debug(" - {{link}} still locked for foreign run {{busy}}", "link", l, "busy", lock)
				// element has been processed after the actual link has been known by the observed state.
				// therefore, it was formally missing and should be handled like this now, afer the element appears
				// but is still processing.
				state.AddMissing(l)
				continue
			}
			log.Debug(" - {{link}} still locked", "link", l)
			state.AddWaiting(l)
			continue
		}
		if t.GetCurrentState().GetOutputVersion() == "" {
			// TODO propgate its state
			if isFinal(t) {
				log.Debug(" - {{link}} not processable and has no output version", "link", l)
			} else {
				log.Debug(" - {{link}} has no output version", "link", l)
			}
			state.AddMissing(l)
			continue
		}
		state.AddWaiting(l)
	}
	return state
}
