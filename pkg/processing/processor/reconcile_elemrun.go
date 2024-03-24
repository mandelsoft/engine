package processor

import (
	"errors"
	"fmt"
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/stringutils"
	"github.com/mandelsoft/logging"
)

type elementRunReconcilation struct {
	*elementReconcilation
	ni *namespaceInfo
}

func newElementRunReconcilation(r *elementReconciler, bctx model.Logging, eid ElementId) *elementRunReconcilation {
	return &elementRunReconcilation{elementReconcilation: newElementReconcilation(r, bctx, eid)}
}

func (r *elementRunReconcilation) Reconcile() pool.Status {
	if r._Element != nil && r.GetStatus() == model.STATUS_DELETED {
		r.Debug("skip deleted element {{element}}")
		return pool.StatusCompleted()
	}

	runid := r.GetLock()
	r.lctx = r.bctx.WithValues("namespace", r.GetNamespace(), "element", r.eid, "runid", runid).WithName(string(runid)).WithName(database.StringId(r.eid))
	r.Logger = r.lctx.Logger().WithValues("status", r.GetStatus())
	r.ni = r.getNamespaceInfo(r.GetNamespace())

	var ready *ReadyState

	mode := "process"
	if r.IsMarkedForDeletion() {
		mode = "delete"
	}
	r.Info("processing element ({{mode}}) {{element}} with status {{status}} (finalizers {{finalizers}})", "finalizers", r.GetObject().GetFinalizers(), "mode", mode)

	var links []ElementId
	var formalVersion string

	curlinks := r.GetCurrentState().GetLinks()
	deletion := false
	if r.IsMarkedForDeletion() {
		err := r.GetObject().PrepareDeletion(r.lctx, newSlaveManagement(r, r.ni, r._Element), r.GetPhase())
		if err != nil {
			return pool.StatusCompleted(err)
		}

		children := r.ni.GetChildren(r.Id())
		if len(children) == 0 {
			r.Info("element {{element}} is deleting and no children found -> initiate deletion")
			r.Info("  found links {{links}}", "links", stringutils.Join(curlinks))
			links = curlinks
			deletion = true
		} else {
			var list []ElementId
			for _, c := range children {
				list = append(list, c.Id())
			}
			r.Info("element {{element}} is deleting but still has children ({{children}}) found -> normal processing", "children", list)
		}
	}
	if !deletion && r.GetLock() == "" {
		if r.GetStatus() == model.STATUS_BLOCKED {
			r.Info("checking ready condition for blocked element {{element}}")
			ready := r.isReady()
			if ready.ReadyForTrigger() {
				r.Info("all links ready for consumption -> initiate new run")
				return r.initiateNewRun()
			}
			r.Info("found still missing elements for {{element}} ({{missing}}) -> keep state blocked", "missing", stringutils.Join(ready.BlockingElements(), ","))
		} else {
			r.Info("no active run for {{element}} -> skip processing")
		}
		return pool.StatusCompleted()
	}

	if !deletion {
		if isExtTriggerable(r._Element) || r.isReTriggerable(r._Element) {
			// wait for inputs to become ready

			if r.GetProcessingState() == nil {
				r.Info("checking current links")
				links = r.GetCurrentState().GetLinks()

				if err := r.verifyLinks(r._Element, links...); err != nil {
					return r.fail(true, err)
				}

				// first, check current state
				ready = r.checkReady("current", r.GetLock(), links)
				if ok := r.notifyCurrentWaitingState(ready); ok {
					// return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
					return pool.StatusCompleted() // TODO: require rate limiting??
				}
			}

			if r.GetProcessingState() == nil || r.isReTriggerable(r._Element) {
				if r.isReTriggerable(r._Element) {
					r.Info("update target state")
				} else {
					r.Info("gather target state")
				}
				// second, assign target state by transferring the current external state to the internal object
				s, err := r.assignTargetState()
				switch s {
				case model.ACCEPT_OK:
					if err != nil {
						r.Error("cannot update external state for internal object", "error", err)
					}

				case model.ACCEPT_INVALID:
					r.Error("external state for internal object invalid -> fail element", "error", err)
					return r.fail(true, err)
				}
				if err != nil {
					return pool.StatusCompleted(err)
				}

				// checking target dependencies after fixing the target state
				r.Info("checking target links and get actual inputs")
				links = r.GetObject().GetTargetState(r.GetPhase()).GetLinks()
				if err := r.verifyLinks(r._Element, links...); err != nil {
					return r.fail(true, err)
				}
			} else {
				r.Info("continue interrupted processing")
				links = r.GetObject().GetTargetState(r.GetPhase()).GetLinks()
			}

			ready = r.checkReady("target", r.GetLock(), links)
			ok, blocked, err := r.notifyTargetWaitingState(ready)
			if err != nil {
				return pool.StatusCompleted(fmt.Errorf("notifying blocked status failed: %w", err))
			}
			if blocked {
				r.pending.Add(-1)
				r.triggerChildren(true)
				r.Info("unresolvable dependencies {{waiting}}", "waiting", stringutils.Join(ready.Waiting))
				return pool.StatusFailed(fmt.Errorf("unresolvable dependencies %s", stringutils.Join(ready.Waiting)))
			}
			if ok {
				return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
				// TODO: trigger waiting by completed element of foreign run !!!!!!!!!!!!!
				r.Info("missing dependencies {{waiting}}", "waiting", stringutils.Join(ready.Waiting))
				return pool.StatusCompleted(nil) // TODO: rate limiting required?
			}

			if r.GetProcessingState() == nil {
				// mark element to be ready by setting the element's target state to the target state of the internal
				// object for the actual phase
				r.SetProcessingState(NewTargetState(r._Element))
			}

			// check effective version for required phase processing.
			target := r.GetObject().GetTargetState(r.GetPhase())

			indiff := diff(r, "input version", r.GetCurrentState().GetInputVersion(), target.GetInputVersion(ready.Inputs))
			obdiff := diff(r, "object version", r.GetCurrentState().GetObjectVersion(), target.GetObjectVersion())
			if !indiff && !obdiff {
				r.Info("effective version unchanged -> skip processing of phase")
				err := r.notifyCompletedState("no processing required", nil, ready.Inputs)
				if err == nil {
					err = r.setStatus(r, r._Element, model.STATUS_COMPLETED, true)
					if err == nil {
						r.pending.Add(-1)
						r.triggerChildren(true)
					}
				}
				return pool.StatusCompleted(err)
			}

			formalVersion = r.formalVersion(ready.Inputs)

			upstate := func(log logging.Logger, o model.ExternalObject) error {
				return o.UpdateStatus(r.lctx, r.Objectbase(), r.Id(), model.StatusUpdate{
					Status:        generics.Pointer(model.STATUS_PROCESSING),
					FormalVersion: generics.Pointer(formalVersion),
					Message:       generics.Pointer(fmt.Sprintf("processing phase %s", r.GetPhase())),
				})
			}

			r.Info("update processing status of external objects")
			err = r.forExtObjects(upstate, UpdateObjects)
			if err != nil {
				return pool.StatusCompleted(err)
			}

			err = r.setStatus(r, r._Element, model.STATUS_PROCESSING)
			if err != nil {
				return pool.StatusCompleted(err)
			}
		} else {
			links = r.GetObject().GetTargetState(r.GetPhase()).GetLinks()
			ready = r.checkReady("target", r.GetLock(), links)
			if !ready.Ready() {
				r.Error("unexpected state of parents, should be available, but found missing {{missing}} and/or waiting {{waiting}}",
					"missing", stringutils.Join(ready.BlockingElements()), "waiting", stringutils.Join(ready.Waiting))
				return r.fail(false, fmt.Errorf("unexpected state of parents"))
			}
			formalVersion = r.formalVersion(ready.Inputs)
		}
	} else {
		err := r.setStatus(r, r._Element, model.STATUS_DELETING)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}

	if !deletion {
		r.Debug("working on formal target version {{formal}}", "formal", formalVersion)
	}

	if isProcessable(r._Element) {
		// now we can process the phase
		r.Info("executing phase {{phase}} of internal object {{intid}} (deletion {{deletion}})", "phase", r.GetPhase(), "intid", r.Id().ObjectId(), "deletion", deletion)
		request := model.Request{
			Logging:         r.lctx,
			Model:           r.Controller().processingModel,
			Element:         r._Element,
			Delete:          deletion,
			FormalVersion:   formalVersion,
			ElementAccess:   r.ni,
			SlaveManagement: newSlaveManagement(r, r.ni, r._Element),
		}
		if ready != nil {
			request.Inputs = ready.Inputs
		}
		result := r.GetObject().Process(request)

		if result.Error != nil {
			if result.Status == model.STATUS_FAILED || result.Status == model.STATUS_INVALID {
				// non-recoverable error, wait for new change in external object state
				r.Error("processing provides non recoverable error", "error", result.Error)
				return r.fail(result.Status == model.STATUS_INVALID, result.Error, formalVersion)
			}
			r.Error("processing provides error", "error", result.Error)
			err := r.updateStatus(result.Status, result.Error.Error(), FormalVersion(formalVersion))
			if err != nil {
				return pool.StatusCompleted(err)
			}
			err = r.setStatus(r, r._Element, result.Status)
			if err != nil {
				return pool.StatusCompleted(err)
			}
			return pool.StatusCompleted(result.Error)
		} else {
			switch result.Status {
			case model.STATUS_FAILED:
				r.setStatus(r, r._Element, model.STATUS_FAILED)
				r.pending.Add(-1)
				r.triggerChildren(true)
			case model.STATUS_DELETED:
				err := r.phaseDeleted()
				if err != nil {
					return pool.StatusCompleted(err)
				}
				r.Controller().events.TriggerStatusEvent(r, r._Element)
				r.Controller().pending.Add(-1)
				r.triggerLinks(r, "parent", links...)
				r.triggerChildren(true)
			case model.STATUS_COMPLETED:
				err := r.notifyCompletedState("processing completed", result.EffectiveObjectVersion, ready.Inputs,
					result.ResultState,
					CalcEffectiveVersion(ready.Inputs, r.GetProcessingState().GetObjectVersion()))
				if err != nil {
					return pool.StatusCompleted(err)
				}
				r.setStatus(r, r._Element, model.STATUS_COMPLETED)
				r.pending.Add(-1)
				r.triggerChildren(true)

				vanished := false
				for _, l := range curlinks {
					if !slices.Contains(links, l) {
						if !vanished {
							r.Info("triggering vanished links")
							vanished = true
						}
						r.Info(" - trigger vanished link {{link}}", "link", l)
					}
					r.EnqueueKey(CMD_ELEM, l)
				}
			default:
				r.setStatus(r, r._Element, result.Status)
			}
		}
	} else {
		r.Info("element with status {{status}} is not processable")
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

func (r *elementRunReconcilation) phaseDeleted() error {
	r.Info("phase {{element}} deleted by processing step")
	err := r.updateStatus(model.STATUS_DELETED, "deleted")
	if err != nil {
		return err
	}
	// delay events
	err = r.setStatus(r, r._Element, model.STATUS_DELETED, false)
	if err != nil {
		return err
	}

	trigger := r.processingModel.MetaModel().GetTriggerTypeForElementType(r.Id().TypeId())
	var ext []model.ExternalObject
	if trigger != nil {
		oid := model.SlaveObjectId(r.Id(), *trigger)
		o, err := r.processingModel.ObjectBase().GetObject(oid)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				return err
			}
			r.Info(" - triggering external object {{oid}} already gone", "oid", oid)
		} else {
			ext = append(ext, o.(model.ExternalObject))
		}
	}
	if len(ext) != 0 {
		for _, o := range ext {
			r.Info(" - removing finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
			ok, err := o.RemoveFinalizer(r.processingModel.ObjectBase(), FINALIZER)
			if err != nil {
				r.LogError(err, "cannot remove finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
				return err
			}
			if !ok {
				r.Info("   no finalizer for triggering external object {{oid}} to remove", "oid", NewObjectIdFor(o))
			}
		}
	}

	var phases_del []Phase
	var phases_val []Phase
	for _, phase := range r.processingModel.MetaModel().Phases(r.GetType()) {
		eid := NewElementIdForPhase(r, phase)
		e := r.ni.GetElement(eid)
		if e != nil {
			if e.GetStatus() == model.STATUS_DELETED {
				phases_del = append(phases_del, e.GetPhase())
			} else {
				phases_val = append(phases_val, e.GetPhase())
			}
		}
	}

	if len(phases_val) > 0 {
		r.Info("found still undeleted phases {{phases}} for internal element", "phases", phases_val)
		r.Info("internal object not deleted")
		return nil
	}
	r.Info("all phases ({{phases}}) in status deleted", "phases", phases_del)

	r.Info("removing finalizer for internal object {{oid}}", "oid", NewObjectIdFor(r.GetObject()))
	_, err = r.GetObject().RemoveFinalizer(r.processingModel.ObjectBase(), FINALIZER)
	if err != nil {
		r.LogError(err, "cannot remove finalizer for internal object {{oid}}", "oid", NewObjectIdFor(r.GetObject()))
		return err
	}
	r.Info("deleting internal object {{oid}}", "oid", NewObjectIdFor(r.GetObject()))
	_, err = r.processingModel.ObjectBase().DeleteObject(r.GetObject())
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			r.LogError(err, "cannot delete internal object {{oid}}", "oid", NewObjectIdFor(r.GetObject()))
			return err
		}
		return err
	}

	r.Info("removing element {{element}} from processing model")
	var children []ElementId
	for _, ph := range r.processingModel.MetaModel().Phases(r.GetType()) {
		for _, c := range r.ni.GetChildren(NewElementIdForPhase(r, ph)) {
			if !slices.Contains(children, c.Id()) {
				children = append(children, c.Id())
			}
		}
	}

	if r.ni.RemoveInternal(r, r.Controller().processingModel, NewObjectIdFor(r)) {
		if r.Controller().processingModel.RemoveNamespace(r, r.ni) {
			r.Controller().events.TriggerNamespaceEvent(r.ni)
		}
	}

	for _, c := range children {
		r.Info("  trigger dependent element {{depelem}}", "depelem", c)
		r.EnqueueKey(CMD_ELEM, c)
	}
	return nil
}

func (r *elementRunReconcilation) notifyCompletedState(msg string, eff *string, inputs model.Inputs, args ...interface{}) error {
	var ci *model.CommitInfo

	formal := r.formalVersion(inputs)
	result := GetResultState(args...)
	target := r.GetProcessingState()
	if result != nil {
		ci = &model.CommitInfo{
			FormalVersion: formal,
			InputVersion:  target.GetInputVersion(inputs),
			ObjectVersion: eff,
			OutputState:   result,
		}
	}
	if target != nil {
		r.Info("committing target state")
		_, err := r.Commit(r.lctx, r.processingModel.ObjectBase(), r.GetLock(), ci)
		if err != nil {
			r.Error("cannot unlock element {{element}}", "error", err)
			return err
		}
		r.SetProcessingState(nil)
	} else {
		r.Info("skipping commit of target state")
	}
	r.Info("completed processing of element {{element}}", "output")
	err := r.updateStatus(model.STATUS_COMPLETED, msg, append(args, RunId(""), FormalVersion(formal))...)
	if err != nil {
		return err
	}
	return nil
}

func (r *elementRunReconcilation) notifyCurrentWaitingState(ready *ReadyState) bool {
	var keys []interface{}

	keys = append(keys, "found", stringutils.Join(maputils.Keys(ready.Inputs, CompareElementId)))
	if !ready.ReadyForTrigger() {
		keys = append(keys, "missing", stringutils.Join(ready.BlockingElements()))
	}
	if len(ready.Waiting) > 0 {
		keys = append(keys, "waiting", stringutils.Join(ready.Waiting))
		r.Info("inputs according to current state not ready", keys...)
		return true
	}
	if !ready.ReadyForTrigger() {
		r.Info("found missing dependencies {{missing}}, but other dependencies ready {{found}} -> continue with target state", keys...)
	} else {
		r.Info("inputs according to current state ready", keys...)
	}
	return false
}

func (r *elementRunReconcilation) notifyTargetWaitingState(ready *ReadyState) (bool, bool, error) {
	var keys []interface{}
	if len(ready.Inputs) > 0 {
		keys = append(keys, "found", stringutils.Join(maputils.Keys(ready.Inputs, CompareElementId)))
	}
	if !ready.ReadyForTrigger() {
		keys = append(keys, "missing", stringutils.Join(ready.BlockingElements()))
	}
	if len(ready.Waiting) > 0 {
		keys = append(keys, "waiting", stringutils.Join(ready.Waiting))
	}
	if !ready.ReadyForTrigger() {
		r.Info("inputs according to target state not ready", keys...)
		return true, true, r.blocked(fmt.Sprintf("unresolved dependencies %s", stringutils.Join(ready.BlockingElements())))
	}
	if len(ready.Waiting) > 0 {
		r.Info("inputs according to target state not ready", keys...)
		return true, false, nil
	}
	r.Info("inputs according to target state ready", keys...)
	return false, false, nil
}

func (r *elementRunReconcilation) block(msg string) pool.Status {
	err := r.blocked(msg)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	r.pending.Add(-1)
	r.triggerChildren(true)
	return pool.StatusCompleted()
}

func (r *elementRunReconcilation) blocked(msg string) error {
	err := r.updateStatus(model.STATUS_BLOCKED, msg, r.GetLock())
	if err == nil {
		_, err = r.Rollback(r.lctx, r.Objectbase(), r.GetLock(), true)
	}
	if err == nil {
		err = r.setStatus(r, r._Element, model.STATUS_BLOCKED)
	}
	return err
}

func (r *elementRunReconcilation) fail(invalid bool, fail error, formal ...string) pool.Status {
	err := r.failed(invalid, fail.Error(), formal...)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	r.Controller().pending.Add(-1)
	r.triggerChildren(true)
	return pool.StatusFailed(fail)
}

func (r *elementRunReconcilation) failed(invalid bool, msg string, formal ...string) error {
	status := model.STATUS_FAILED
	if invalid {
		status = model.STATUS_INVALID
	}
	opts := []interface{}{
		r.GetLock(),
	}
	if len(formal) > 0 && formal[0] != "" {
		opts = append(opts, FormalVersion(formal[0]))
	}
	err := r.updateStatus(status, msg, opts...)
	if err == nil {
		_, err = r.Rollback(r.lctx, r.Objectbase(), r.GetLock(), true, formal...)
	}
	if err == nil {
		err = r.setStatus(r, r._Element, status)
	}
	return err
}

func (r *elementRunReconcilation) assignTargetState() (model.AcceptStatus, error) {
	// determine potential external objects
	if r.GetObject().GetTargetState(r.GetPhase()) != nil {
		r.Info("target state for internal object of {{element}} already set for actual phase -> update state")
	}

	var extstate model.ExternalState

	mod := func(log logging.Logger, o model.ExternalObject) error {
		if isProcessable(r._Element) && !r.isReTriggerable(r._Element, o.GetType()) {
			return nil
		}
		state := r.GetExternalState(o)
		v := state.GetVersion()
		log.Debug("  found effective external state from {{extid}} for phase {{phase}}: {{state}}",
			"phase", r.GetPhase(), "state", general.DescribeObject(state))
		err := o.UpdateStatus(r.lctx, r.Objectbase(), r.Id(), model.StatusUpdate{
			RunId:           generics.Pointer(r.GetLock()),
			DetectedVersion: &v,
			ObservedVersion: nil,
			Status:          generics.Pointer(model.STATUS_PREPARING),
			Message:         generics.Pointer("preparing target state"),
			ExternalState:   state,
			ResultState:     nil,
		})
		if err != nil {
			r.setStatus(r, r._Element, model.STATUS_PREPARING)
			log.Error("cannot update status for external object {{extid}}", "error", err)
			return err
		}
		extstate = state
		return nil
	}

	r.Info("gathering state for external object types")
	err := r.forExtObjects(mod, TriggeringObject)
	if err != nil {
		return model.ACCEPT_OK, err
	}

	if extstate == nil {
		r.Info("no external object states found for {{element}}  -> propagate empty state")
	} else {
		r.Info("assigning external state for processing {{element}}", "extstate", general.DescribeObject(extstate))
	}
	s, err := r.GetObject().AcceptExternalState(r.lctx, r.Objectbase(), r.GetPhase(), extstate)
	if s != model.ACCEPT_OK || err != nil {
		return s, err
	}
	if extstate != nil {
		r.Info("assigned state for phase {{phase}} from type {{type}} to {{version}}",
			"phase", r.GetPhase(),
			"type", *r.MetaModel().GetTriggerTypeForElementType(r.Id().TypeId()),
			"version", extstate.GetVersion())
	}
	return s, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *elementRunReconcilation) isReady() *ReadyState {
	links := r.GetCurrentState().GetObservedState().GetLinks()
	return r.checkReady("observed", r.GetLock(), links)
}

func (r *elementRunReconcilation) checkReady(kind string, lock RunId, links []ElementId) *ReadyState {
	state := NewReadyState()

	r.Debug(fmt.Sprintf("evaluating %s links {{links}}", kind), "links", links)
	r.ni.lock.Lock()
	defer r.ni.lock.Unlock()

	for _, l := range links {
		t := r.ni.elements[l]
		if t == nil {
			r.Debug(" - {{link}} not found", "link", l)
			state.AddMissing(l)
			continue
		}
		if t.GetLock() == "" && t.GetCurrentState().GetOutputVersion() != "" {
			state.AddInput(l, t.GetCurrentState().GetOutput())
			r.Debug(" - {{link}} is unlocked and has output state", "link", l)
			continue
		}
		if t.GetLock() != "" {
			if lock != "" && t.GetLock() != lock {
				r.Debug(" - {{link}} still locked for foreign run {{busy}}", "link", l, "busy", t.GetLock())
				// element has been processed after the actual link has been known by the observed state.
				// therefore, it was formally missing and should be handled like this now, afer the element appears
				// but is still processing.
				state.AddMissing(l)
				continue
			}
			r.Debug(" - {{link}} still locked", "link", l)
			state.AddWaiting(l)
			continue
		}
		if t.GetCurrentState().GetOutputVersion() == "" {
			// TODO propgate its state
			if isFinal(t) {
				r.Debug(" - {{link}} not processable and has no output version", "link", l)
			} else {
				r.Debug(" - {{link}} has no output version", "link", l)
			}
			state.AddMissing(l)
			continue
		}
		state.AddWaiting(l)
	}
	return state
}

func (r *elementReconcilation) forExtObjects(f func(log logging.Logger, object model.ExternalObject) error, set func(c *Controller, id TypeId) []string) error {
	exttypes := set(r.Controller(), r.Id().TypeId())
	for _, t := range exttypes {
		id := NewObjectId(t, r.GetNamespace(), r.GetName())
		log := r.WithValues("extid", id)
		_o, err := r.processingModel.ObjectBase().GetObject(database.NewObjectId(id.GetType(), id.GetNamespace(), id.GetName()))
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				log.Error("cannot get external object {{extid}}", "error", err)
				return err
			}
			log.Info("external object {{extid}} not found -> skip")
			continue
		}
		err = f(log, _o.(model.ExternalObject))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *elementRunReconcilation) updateStatus(status model.Status, message string, args ...any) error {
	update := model.StatusUpdate{
		Status:  &status,
		Message: &message,
	}
	keys := []interface{}{
		"newstatus", status,
		"message", message,
	}

	for _, a := range args {
		switch opt := a.(type) {
		case RunId:
			update.RunId = generics.Pointer(opt)
			keys = append(keys, "runid", update.RunId)
		case model.OutputState:
			update.ResultState = opt
			keys = append(keys, "result", general.DescribeObject(opt))
		case FormalVersion:
			update.FormalVersion = generics.Pointer(string(opt))
			keys = append(keys, "formal version", opt)
		case ObservedVersion:
			update.ObservedVersion = generics.Pointer(string(opt))
			keys = append(keys, "observed version", opt)
		case DetectedVersion:
			update.DetectedVersion = generics.Pointer(string(opt))
			keys = append(keys, "detected version", opt)
		case EffectiveVersion:
			update.EffectiveVersion = generics.Pointer(string(opt))
			keys = append(keys, "effective version", opt)
		default:
			panic(fmt.Sprintf("unknown status argument type %T", a))
		}
	}
	r.Info(" updating status of external objects to {{newstatus}}: {{message}}", keys...)

	mod := func(log logging.Logger, o model.ExternalObject) error {
		return o.UpdateStatus(r.lctx, r.Objectbase(), r.Id(), update)
	}
	return r.forExtObjects(mod, UpdateObjects)
}

func (r *elementRunReconcilation) triggerChildren(release bool) {
	r.ni.lock.Lock()
	defer r.ni.lock.Unlock()
	// TODO: dependency check must be synchronized with this trigger

	id := r.eid
	r.Info("triggering children for {{element}} (checking {{amount}} elements in namespace)", "amount", len(r.ni.elements))
	for _, e := range r.ni.elements {
		if e.GetProcessingState() != nil {
			links := e.GetProcessingState().GetLinks()
			r.Debug("  elem {{child}} has target links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					r.Info("- trigger pending element {{waiting}} active in {{target-runid}}", "waiting", e.Id(), "target-runid", e.GetLock())
					r.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		} else if e.GetStatus() != model.STATUS_DELETED && e.GetCurrentState() != nil {
			links := e.GetCurrentState().GetObservedState().GetLinks()
			r.Debug("  elem {{child}} has current links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					r.Info("- trigger pending element {{waiting}}", "waiting", e.Id(), "target-runid", e.GetLock())
					r.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		}
	}
	if release {
		r._Element.SetProcessingState(nil)
	}
}

func (r *elementRunReconcilation) formalVersion(inputs model.Inputs) string {
	n := version.NewNode(r.Id().TypeId(), r.GetName(), r.GetTargetState().GetFormalObjectVersion())
	return r.composer.Compose(n, formalInputVersions(inputs)...)
}
