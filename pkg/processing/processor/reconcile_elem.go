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
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/stringutils"
	"github.com/mandelsoft/logging"
)

type elementReconciler struct {
	*reconciler
}

func newElementReconciler(c *Controller) *elementReconciler {
	return &elementReconciler{&reconciler{controller: c}}
}

func (r *elementReconciler) Command(p pool.Pool, ctx pool.MessageContext, command pool.Command) pool.Status {
	// ctx = logging.ExcludeFromMessageContext[logging.Realm](ctx)
	ctx = ctx.WithContext(REALM)
	cmd, _, id := DecodeCommand(command)
	if id != nil {
		return newElementReconcilation(r, ctx, cmd, *id).Reconcile()
	} else {
		return pool.StatusFailed(fmt.Errorf("invalid element command %q", command))
	}
}

type elementReconcilation struct {
	*elementReconciler
	logging.Logger
	_Element

	cmd  string
	eid  ElementId
	lctx model.Logging
	bctx model.Logging
	ni   *namespaceInfo
}

func newElementReconcilation(r *elementReconciler, bctx model.Logging, cmd string, eid ElementId) *elementReconcilation {
	lctx := bctx.WithValues("namespace", eid.GetNamespace(), "element", eid).WithName(eid.String())
	log := bctx.Logger()
	elem := r.processingModel._GetElement(eid)

	return &elementReconcilation{
		elementReconciler: r,
		Logger:            log,
		_Element:          elem,

		cmd:  cmd,
		lctx: lctx,
		bctx: bctx,
		eid:  eid,
	}
}

func (r *elementReconcilation) LoggingContext() model.Logging {
	return r.LoggingContext()
}

func (p *elementReconcilation) Reconcile() pool.Status {
	if p.Controller().delay > 0 {
		time.Sleep(p.Controller().delay)
	}

	p._Element.GetStatus()

	if p._Element != nil && p.GetStatus() == model.STATUS_DELETED {
		p.Debug("skip deleted element {{element}}")
		return pool.StatusCompleted()
	}

	if p._Element == nil {
		if p.cmd != CMD_EXT {
			return pool.StatusFailed(fmt.Errorf("unknown element %q", p.eid))
		}
		var status pool.Status
		status = p.handleNew()
		if p._Element == nil {
			return status
		}
	}

	runid := p.GetLock()
	if p.cmd == CMD_EXT {
		if runid == "" || p.isReTriggerable(p._Element) || p.IsMarkedForDeletion() {
			if runid == "" {
				p.Debug("new external change for {{element}} (deletion requested: {{marked}})", "marked", p.IsMarkedForDeletion())
			} else {
				p.Debug("retriggering external change for {{element}} (deletion requested: {{marked}})", "marked", p.IsMarkedForDeletion())
			}
			return p.handleExternalChange()
		}
		p.Debug("skip external object trigger for {{element}}")
		return pool.StatusCompleted()
	} else {
		p.lctx = p.bctx.WithValues("namespace", p.GetNamespace(), "element", p.eid, "runid", runid).WithName(string(runid)).WithName(database.StringId(p.eid))
		p.Logger = p.lctx.Logger()
		p.ni = p.getNamespaceInfo(p.GetNamespace())
		return p.handleRun()
	}

}

func (p *elementReconcilation) handleNew() pool.Status {
	p.Info("processing unknown element {{element}}")

	_i, err := p.Objectbase().GetObject(p.eid)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return pool.StatusCompleted(err)
		}
		p.Info("internal object not found for {{element}}")

		_, ext, err := p.getTriggeringExternalObject(p.eid)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		if ext == nil {
			p.Info("no triggering object found -> obsolete event -> don't create new element")
			return pool.StatusCompleted()
		}
		if p.isDeleting(ext) {
			p.Info("external object is deleting -> don't create new element")
			return pool.StatusCompleted()
		}

		_i, err = p.processingModel.ObjectBase().CreateObject(p.eid)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}
	i := _i.(model.InternalObject)

	p.ni, err = p.Controller().processingModel.AssureNamespace(p.Logger, i.GetNamespace(), true)
	if err != nil {
		return pool.StatusCompleted(err)
	}

	p._Element = p.ni._AddElement(i, p.GetPhase())
	return pool.StatusCompleted()
}

type Value struct {
	msg string
}

func (p *elementReconcilation) handleExternalChange() pool.Status {
	p.Info("processing external element trigger for {{element}} with status {{status}}", "status", p.GetStatus())

	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(p.Id().TypeId())
	if trigger == nil {
		p.Info("no triggering types for {{element}}")
		return pool.StatusCompleted()
	}

	if !isExtTriggerable(p._Element) {
		if !p.isReTriggerable(p._Element, *trigger) {
			if !p.IsMarkedForDeletion() {
				p.Info("state for element in status {{status}} is already assigned", "status", p.GetStatus())
				return pool.StatusCompleted()
			}
		}
		p.Info("state for element in status {{status}} is already assigned but retriggerable", "status", p.GetStatus())
	}

	p.Info("checking state of external objects for element {{element}}", "exttypes", trigger)
	var changed *string
	deleting := false
	cur := p.GetCurrentState().GetObservedState().GetObjectVersion()

	id := database.NewObjectId(*trigger, p.GetNamespace(), p.GetName())
	p.Logger = p.WithValues("extid", id)
	_o, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			p.LogError(err, "cannot get external object {{extid}}")
			return pool.StatusCompleted(fmt.Errorf("cannot get external object %s: %w", id, err))
		}
		p.Info("external object {{extid}} not found -> ignore state")
		return pool.StatusCompleted()
	}

	o := _o.(model.ExternalObject)
	if o.IsDeleting() {
		p.Info("external object {{extid}} requests deletion")
		deleting = true
	}

	// give the internal object the chance to modify the actual external state
	ov := o.GetState().GetVersion()
	es := p.GetExternalState(o)
	v := es.GetVersion()
	if ov != v {
		p.Info("state of external object {{extid}} adjusted from {{objectversion}} to {{version}}", "objectversion", ov, "version", v)
		p.Debug("external state: {{state}}", "state", general.DescribeObject(es))
	}
	if v == cur {
		p.Info("state of external object {{extid}} not changed ({{version}})", "version", v)
	} else {
		changed = trigger
		p.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
	}

	if changed == nil && !deleting {
		p.Info("no external object state change found for {{element}}")
		return pool.StatusCompleted()
	}

	var leafs []Phase
	var phases []Phase
	if deleting {
		var ok bool
		var err error
		p.Info("element {{element}} should be deleted")
		ok, phases, leafs, err = p.MarkForDeletion(p.processingModel)
		if err != nil {
			p.LogError(err, "cannot mark all dependent phases ({phases}}) of {{element}} as deleting", "phases", phases)
			return pool.StatusCompleted(err)
		}
		if ok {
			p.Info("marked dependent phases ({{phases}}) of {{element}} as deleting", "phases", phases)
		}
	}

	if changed != nil && p.isReTriggerable(p._Element, *changed) {
		p.Info("retrigger state change of external object {{extid}} for {{element}}")
		p.EnqueueKey(CMD_ELEM, p.eid)
		return pool.StatusCompleted()
	}

	if changed != nil {
		return p.initiateNewRun()
	}

	p.Info("triggering leaf phases {{phases}} for deletion", "phases", leafs)
	for _, phase := range leafs {
		id := NewElementIdForPhase(p.eid, phase)
		p.Debug(" - triggering {{leaf}}", "leaf", id)
		p.EnqueueKey(CMD_ELEM, id)
	}
	return pool.StatusCompleted()
}

func (p *elementReconcilation) initiateNewRun() pool.Status {
	p.Info("trying to initiate new run for {{element}}")
	ni := p.getNamespaceInfo(p.GetNamespace())
	rid, err := ni.lockGraph(p, p._Element)
	if err == nil {
		if rid != nil {
			p.Info("starting run {{runid}}", "runid", *rid)
			p.EnqueueKey(CMD_ELEM, p.eid)
		} else {
			err = fmt.Errorf("delay initiation of new run")
		}
	}
	return pool.StatusCompleted(err)
}

func (p *elementReconcilation) isReTriggerable(e _Element, ext ...string) bool {
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

func (p *elementReconcilation) handleRun() pool.Status {

	var ready *ReadyState

	p.Logger = p.WithValues("status", p.GetStatus())
	mode := "process"
	if p.IsMarkedForDeletion() {
		mode = "delete"
	}
	p.Info("processing element ({{mode}}) {{element}} with status {{status}} (finalizers {{finalizers}})", "finalizers", p.GetObject().GetFinalizers(), "mode", mode)

	var links []ElementId
	var formalVersion string

	curlinks := p.GetCurrentState().GetLinks()
	deletion := false
	if p.IsMarkedForDeletion() {
		err := p.GetObject().PrepareDeletion(p.lctx, newSlaveManagement(p, p.ni, p._Element), p.GetPhase())
		if err != nil {
			return pool.StatusCompleted(err)
		}

		children := p.ni.GetChildren(p.Id())
		if len(children) == 0 {
			p.Info("element {{element}} is deleting and no children found -> initiate deletion")
			p.Info("  found links {{links}}", "links", stringutils.Join(curlinks))
			links = curlinks
			deletion = true
		} else {
			var list []ElementId
			for _, c := range children {
				list = append(list, c.Id())
			}
			p.Info("element {{element}} is deleting but still has children ({{children}}) found -> normal processing", "children", list)
		}
	}
	if !deletion && p.GetLock() == "" {
		if p.GetStatus() == model.STATUS_BLOCKED {
			p.Info("checking ready condition for blocked element {{element}}")
			ready := p.isReady()
			if ready.ReadyForTrigger() {
				p.Info("all links ready for consumption -> initiate new run")
				return p.initiateNewRun()
			}
			p.Info("found still missing elements for {{element}} ({{missing}}) -> keep state blocked", "missing", stringutils.Join(ready.BlockingElements(), ","))
		} else {
			p.Info("no active run for {{element}} -> skip processing")
		}
		return pool.StatusCompleted()
	}

	if !deletion {
		if isExtTriggerable(p._Element) || p.isReTriggerable(p._Element) {
			// wait for inputs to become ready

			if p.GetProcessingState() == nil {
				p.Info("checking current links")
				links = p.GetCurrentState().GetLinks()

				if err := p.verifyLinks(p._Element, links...); err != nil {
					return p.fail(true, err)
				}

				// first, check current state
				ready = p.checkReady("current", p.GetLock(), links)
				if ok := p.notifyCurrentWaitingState(ready); ok {
					// return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
					return pool.StatusCompleted() // TODO: require rate limiting??
				}
			}

			if p.GetProcessingState() == nil || p.isReTriggerable(p._Element) {
				if p.isReTriggerable(p._Element) {
					p.Info("update target state")
				} else {
					p.Info("gather target state")
				}
				// second, assign target state by transferring the current external state to the internal object
				s, err := p.assignTargetState()
				switch s {
				case model.ACCEPT_OK:
					if err != nil {
						p.Error("cannot update external state for internal object", "error", err)
					}

				case model.ACCEPT_INVALID:
					p.Error("external state for internal object invalid -> fail element", "error", err)
					return p.fail(true, err)
				}
				if err != nil {
					return pool.StatusCompleted(err)
				}

				// checking target dependencies after fixing the target state
				p.Info("checking target links and get actual inputs")
				links = p.GetObject().GetTargetState(p.GetPhase()).GetLinks()
				if err := p.verifyLinks(p._Element, links...); err != nil {
					return p.fail(true, err)
				}
			} else {
				p.Info("continue interrupted processing")
				links = p.GetObject().GetTargetState(p.GetPhase()).GetLinks()
			}

			ready = p.checkReady("target", p.GetLock(), links)
			ok, blocked, err := p.notifyTargetWaitingState(ready)
			if err != nil {
				return pool.StatusCompleted(fmt.Errorf("notifying blocked status failed: %w", err))
			}
			if blocked {
				p.pending.Add(-1)
				p.triggerChildren(true)
				p.Info("unresolvable dependencies {{waiting}}", "waiting", stringutils.Join(ready.Waiting))
				return pool.StatusFailed(fmt.Errorf("unresolvable dependencies %s", stringutils.Join(ready.Waiting)))
			}
			if ok {
				return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
				// TODO: trigger waiting by completed element of foreign run !!!!!!!!!!!!!
				p.Info("missing dependencies {{waiting}}", "waiting", stringutils.Join(ready.Waiting))
				return pool.StatusCompleted(nil) // TODO: rate limiting required?
			}

			if p.GetProcessingState() == nil {
				// mark element to be ready by setting the element's target state to the target state of the internal
				// object for the actual phase
				p.SetProcessingState(NewTargetState(p._Element))
			}

			// check effective version for required phase processing.
			target := p.GetObject().GetTargetState(p.GetPhase())

			indiff := diff(p, "input version", p.GetCurrentState().GetInputVersion(), target.GetInputVersion(ready.Inputs))
			obdiff := diff(p, "object version", p.GetCurrentState().GetObjectVersion(), target.GetObjectVersion())
			if !indiff && !obdiff {
				p.Info("effective version unchanged -> skip processing of phase")
				err := p.notifyCompletedState("no processing required", nil, ready.Inputs)
				if err == nil {
					_, err = p.SetStatus(p.processingModel.ObjectBase(), model.STATUS_COMPLETED)
					if err == nil {
						p.pending.Add(-1)
						p.triggerChildren(true)
					}
				}
				return pool.StatusCompleted(err)
			}

			formalVersion = p.formalVersion(ready.Inputs)

			upstate := func(log logging.Logger, o model.ExternalObject) error {
				return o.UpdateStatus(p.lctx, p.Objectbase(), p.Id(), model.StatusUpdate{
					Status:        generics.Pointer(model.STATUS_PROCESSING),
					FormalVersion: generics.Pointer(formalVersion),
					Message:       generics.Pointer(fmt.Sprintf("processing phase %s", p.GetPhase())),
				})
			}

			p.Info("update processing status of external objects")
			err = p.forExtObjects(upstate, UpdateObjects)
			if err != nil {
				return pool.StatusCompleted(err)
			}

			err = p.setStatus(p, p._Element, model.STATUS_PROCESSING)
			if err != nil {
				return pool.StatusCompleted(err)
			}
		} else {
			links = p.GetObject().GetTargetState(p.GetPhase()).GetLinks()
			ready = p.checkReady("target", p.GetLock(), links)
			if !ready.Ready() {
				p.Error("unexpected state of parents, should be available, but found missing {{missing}} and/or waiting {{waiting}}",
					"missing", stringutils.Join(ready.BlockingElements()), "waiting", stringutils.Join(ready.Waiting))
				return p.fail(false, fmt.Errorf("unexpected state of parents"))
			}
			formalVersion = p.formalVersion(ready.Inputs)
		}
	} else {
		err := p.setStatus(p, p._Element, model.STATUS_DELETING)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}

	if !deletion {
		p.Debug("working on formal target version {{formal}}", "formal", formalVersion)
	}

	if isProcessable(p._Element) {
		// now we can process the phase
		p.Info("executing phase {{phase}} of internal object {{intid}} (deletion {{deletion}})", "phase", p.GetPhase(), "intid", p.Id().ObjectId(), "deletion", deletion)
		request := model.Request{
			Logging:         p.lctx,
			Model:           p.Controller().processingModel,
			Element:         p._Element,
			Delete:          deletion,
			FormalVersion:   formalVersion,
			ElementAccess:   p.ni,
			SlaveManagement: newSlaveManagement(p, p.ni, p._Element),
		}
		if ready != nil {
			request.Inputs = ready.Inputs
		}
		result := p.GetObject().Process(request)

		if result.Error != nil {
			if result.Status == model.STATUS_FAILED || result.Status == model.STATUS_INVALID {
				// non-recoverable error, wait for new change in external object state
				p.Error("processing provides non recoverable error", "error", result.Error)
				return p.fail(result.Status == model.STATUS_INVALID, result.Error, formalVersion)
			}
			p.Error("processing provides error", "error", result.Error)
			err := p.updateStatus(result.Status, result.Error.Error(), FormalVersion(formalVersion))
			if err != nil {
				return pool.StatusCompleted(err)
			}
			err = p.setStatus(p, p._Element, result.Status)
			if err != nil {
				return pool.StatusCompleted(err)
			}
			return pool.StatusCompleted(result.Error)
		} else {
			switch result.Status {
			case model.STATUS_FAILED:
				p.setStatus(p, p._Element, model.STATUS_FAILED)
				p.pending.Add(-1)
				p.triggerChildren(true)
			case model.STATUS_DELETED:
				err := p.phaseDeleted()
				if err != nil {
					return pool.StatusCompleted(err)
				}
				p.Controller().events.TriggerStatusEvent(p, p._Element)
				p.Controller().pending.Add(-1)
				p.triggerLinks(p, "parent", links...)
				p.triggerChildren(true)
			case model.STATUS_COMPLETED:
				err := p.notifyCompletedState("processing completed", result.EffectiveObjectVersion, ready.Inputs,
					result.ResultState,
					CalcEffectiveVersion(ready.Inputs, p.GetProcessingState().GetObjectVersion()))
				if err != nil {
					return pool.StatusCompleted(err)
				}
				p.setStatus(p, p._Element, model.STATUS_COMPLETED)
				p.pending.Add(-1)
				p.triggerChildren(true)

				vanished := false
				for _, l := range curlinks {
					if !slices.Contains(links, l) {
						if !vanished {
							p.Info("triggering vanished links")
							vanished = true
						}
						p.Info(" - trigger vanished link {{link}}", "link", l)
					}
					p.EnqueueKey(CMD_ELEM, l)
				}
			default:
				p.setStatus(p, p._Element, result.Status)
			}
		}
	} else {
		p.Info("element with status {{status}} is not processable")
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

func (p *Controller) setupNewInternalObject(log logging.Logger, ni *namespaceInfo, i model.InternalObject, phase Phase, runid RunId) Element {
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

func (p *elementReconcilation) phaseDeleted() error {
	p.Info("phase {{element}} deleted by processing step")
	err := p.updateStatus(model.STATUS_DELETED, "deleted")
	if err != nil {
		return err
	}
	// delay events
	err = p.setStatus(p, p._Element, model.STATUS_DELETED, false)
	if err != nil {
		return err
	}

	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(p.Id().TypeId())
	var ext []model.ExternalObject
	if trigger != nil {
		oid := model.SlaveObjectId(p.Id(), *trigger)
		o, err := p.processingModel.ObjectBase().GetObject(oid)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				return err
			}
			p.Info(" - triggering external object {{oid}} already gone", "oid", oid)
		} else {
			ext = append(ext, o.(model.ExternalObject))
		}
	}
	if len(ext) != 0 {
		for _, o := range ext {
			p.Info(" - removing finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
			ok, err := o.RemoveFinalizer(p.processingModel.ObjectBase(), FINALIZER)
			if err != nil {
				p.LogError(err, "cannot remove finalizer for triggering external object {{oid}}", "oid", NewObjectIdFor(o))
				return err
			}
			if !ok {
				p.Info("   no finalizer for triggering external object {{oid}} to remove", "oid", NewObjectIdFor(o))
			}
		}
	}

	var phases_del []Phase
	var phases_val []Phase
	for _, phase := range p.processingModel.MetaModel().Phases(p.GetType()) {
		eid := NewElementIdForPhase(p, phase)
		e := p.ni.GetElement(eid)
		if e != nil {
			if e.GetStatus() == model.STATUS_DELETED {
				phases_del = append(phases_del, e.GetPhase())
			} else {
				phases_val = append(phases_val, e.GetPhase())
			}
		}
	}

	if len(phases_val) > 0 {
		p.Info("found still undeleted phases {{phases}} for internal element", "phases", phases_val)
		p.Info("internal object not deleted")
		return nil
	}
	p.Info("all phases ({{phases}}) in status deleted", "phases", phases_del)

	p.Info("removing finalizer for internal object {{oid}}", "oid", NewObjectIdFor(p.GetObject()))
	_, err = p.GetObject().RemoveFinalizer(p.processingModel.ObjectBase(), FINALIZER)
	if err != nil {
		p.LogError(err, "cannot remove finalizer for internal object {{oid}}", "oid", NewObjectIdFor(p.GetObject()))
		return err
	}
	p.Info("deleting internal object {{oid}}", "oid", NewObjectIdFor(p.GetObject()))
	_, err = p.processingModel.ObjectBase().DeleteObject(p.GetObject())
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			p.LogError(err, "cannot delete internal object {{oid}}", "oid", NewObjectIdFor(p.GetObject()))
			return err
		}
		return err
	}

	p.Info("removing element {{element}} from processing model")
	var children []ElementId
	for _, ph := range p.processingModel.MetaModel().Phases(p.GetType()) {
		for _, c := range p.ni.GetChildren(NewElementIdForPhase(p, ph)) {
			if !slices.Contains(children, c.Id()) {
				children = append(children, c.Id())
			}
		}
	}

	if p.ni.RemoveInternal(p, p.Controller().processingModel, NewObjectIdFor(p)) {
		if p.Controller().processingModel.RemoveNamespace(p, p.ni) {
			p.Controller().events.TriggerNamespaceEvent(p.ni)
		}
	}

	for _, c := range children {
		p.Info("  trigger dependent element {{depelem}}", "depelem", c)
		p.EnqueueKey(CMD_ELEM, c)
	}
	return nil
}

func (p *elementReconcilation) notifyCompletedState(msg string, eff *string, inputs model.Inputs, args ...interface{}) error {
	var ci *model.CommitInfo

	formal := p.formalVersion(inputs)
	result := GetResultState(args...)
	target := p.GetProcessingState()
	if result != nil {
		ci = &model.CommitInfo{
			FormalVersion: formal,
			InputVersion:  target.GetInputVersion(inputs),
			ObjectVersion: eff,
			OutputState:   result,
		}
	}
	if target != nil {
		p.Info("committing target state")
		_, err := p.Commit(p.lctx, p.processingModel.ObjectBase(), p.GetLock(), ci)
		if err != nil {
			p.Error("cannot unlock element {{element}}", "error", err)
			return err
		}
		p.SetProcessingState(nil)
	} else {
		p.Info("skipping commit of target state")
	}
	p.Info("completed processing of element {{element}}", "output")
	err := p.updateStatus(model.STATUS_COMPLETED, msg, append(args, RunId(""), FormalVersion(formal))...)
	if err != nil {
		return err
	}
	return nil
}

func (p *elementReconcilation) notifyCurrentWaitingState(ready *ReadyState) bool {
	var keys []interface{}

	keys = append(keys, "found", stringutils.Join(maputils.Keys(ready.Inputs, CompareElementId)))
	if !ready.ReadyForTrigger() {
		keys = append(keys, "missing", stringutils.Join(ready.BlockingElements()))
	}
	if len(ready.Waiting) > 0 {
		keys = append(keys, "waiting", stringutils.Join(ready.Waiting))
		p.Info("inputs according to current state not ready", keys...)
		return true
	}
	if !ready.ReadyForTrigger() {
		p.Info("found missing dependencies {{missing}}, but other dependencies ready {{found}} -> continue with target state", keys...)
	} else {
		p.Info("inputs according to current state ready", keys...)
	}
	return false
}

func (p *elementReconcilation) notifyTargetWaitingState(ready *ReadyState) (bool, bool, error) {
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
		p.Info("inputs according to target state not ready", keys...)
		return true, true, p.blocked(fmt.Sprintf("unresolved dependencies %s", stringutils.Join(ready.BlockingElements())))
	}
	if len(ready.Waiting) > 0 {
		p.Info("inputs according to target state not ready", keys...)
		return true, false, nil
	}
	p.Info("inputs according to target state ready", keys...)
	return false, false, nil
}

func (p *elementReconcilation) block(msg string) pool.Status {
	err := p.blocked(msg)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	p.pending.Add(-1)
	p.triggerChildren(true)
	return pool.StatusCompleted()
}

func (p *elementReconcilation) blocked(msg string) error {
	err := p.updateStatus(model.STATUS_BLOCKED, msg, p.GetLock())
	if err == nil {
		_, err = p.Rollback(p.lctx, p.Objectbase(), p.GetLock(), true)
	}
	if err == nil {
		err = p.setStatus(p, p._Element, model.STATUS_BLOCKED)
	}
	return err
}

func (p *elementReconcilation) fail(invalid bool, fail error, formal ...string) pool.Status {
	err := p.failed(invalid, fail.Error(), formal...)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	p.Controller().pending.Add(-1)
	p.triggerChildren(true)
	return pool.StatusFailed(fail)

}

func (p *elementReconcilation) failed(invalid bool, msg string, formal ...string) error {
	status := model.STATUS_FAILED
	if invalid {
		status = model.STATUS_INVALID
	}
	opts := []interface{}{
		p.GetLock(),
	}
	if len(formal) > 0 && formal[0] != "" {
		opts = append(opts, FormalVersion(formal[0]))
	}
	err := p.updateStatus(status, msg, opts...)
	if err == nil {
		_, err = p.Rollback(p.lctx, p.Objectbase(), p.GetLock(), true, formal...)
	}
	if err == nil {
		err = p.setStatus(p, p._Element, status)
	}
	return err
}

func (p *elementReconcilation) assignTargetState() (model.AcceptStatus, error) {
	// determine potential external objects
	if p.GetObject().GetTargetState(p.GetPhase()) != nil {
		p.Info("target state for internal object of {{element}} already set for actual phase -> update state")
	}

	var extstate model.ExternalState

	mod := func(log logging.Logger, o model.ExternalObject) error {
		if isProcessable(p._Element) && !p.isReTriggerable(p._Element, o.GetType()) {
			return nil
		}
		state := p.GetExternalState(o)
		v := state.GetVersion()
		log.Debug("  found effective external state from {{extid}} for phase {{phase}}: {{state}}",
			"phase", p.GetPhase(), "state", general.DescribeObject(state))
		err := o.UpdateStatus(p.lctx, p.Objectbase(), p.Id(), model.StatusUpdate{
			RunId:           generics.Pointer(p.GetLock()),
			DetectedVersion: &v,
			ObservedVersion: nil,
			Status:          generics.Pointer(model.STATUS_PREPARING),
			Message:         generics.Pointer("preparing target state"),
			ExternalState:   state,
			ResultState:     nil,
		})
		if err != nil {
			p.setStatus(p, p._Element, model.STATUS_PREPARING)
			log.Error("cannot update status for external object {{extid}}", "error", err)
			return err
		}
		extstate = state
		return nil
	}

	p.Info("gathering state for external object types")
	err := p.forExtObjects(mod, TriggeringObject)
	if err != nil {
		return model.ACCEPT_OK, err
	}

	if extstate == nil {
		p.Info("no external object states found for {{element}}  -> propagate empty state")
	} else {
		p.Info("assigning external state for processing {{element}}", "extstate", general.DescribeObject(extstate))
	}
	s, err := p.GetObject().AcceptExternalState(p.lctx, p.Objectbase(), p.GetPhase(), extstate)
	if s != model.ACCEPT_OK || err != nil {
		return s, err
	}
	if extstate != nil {
		p.Info("assigned state for phase {{phase}} from type {{type}} to {{version}}",
			"phase", p.GetPhase(),
			"type", *p.MetaModel().GetTriggerTypeForElementType(p.Id().TypeId()),
			"version", extstate.GetVersion())
	}
	return s, nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *elementReconcilation) isReady() *ReadyState {
	links := p.GetCurrentState().GetObservedState().GetLinks()
	return p.checkReady("observed", p.GetLock(), links)
}

func (p *elementReconcilation) checkReady(kind string, lock RunId, links []ElementId) *ReadyState {
	state := NewReadyState()

	p.Debug(fmt.Sprintf("evaluating %s links {{links}}", kind), "links", links)
	p.ni.lock.Lock()
	defer p.ni.lock.Unlock()

	for _, l := range links {
		t := p.ni.elements[l]
		if t == nil {
			p.Debug(" - {{link}} not found", "link", l)
			state.AddMissing(l)
			continue
		}
		if t.GetLock() == "" && t.GetCurrentState().GetOutputVersion() != "" {
			state.AddInput(l, t.GetCurrentState().GetOutput())
			p.Debug(" - {{link}} is unlocked and has output state", "link", l)
			continue
		}
		if t.GetLock() != "" {
			if lock != "" && t.GetLock() != lock {
				p.Debug(" - {{link}} still locked for foreign run {{busy}}", "link", l, "busy", t.GetLock())
				// element has been processed after the actual link has been known by the observed state.
				// therefore, it was formally missing and should be handled like this now, afer the element appears
				// but is still processing.
				state.AddMissing(l)
				continue
			}
			p.Debug(" - {{link}} still locked", "link", l)
			state.AddWaiting(l)
			continue
		}
		if t.GetCurrentState().GetOutputVersion() == "" {
			// TODO propgate its state
			if isFinal(t) {
				p.Debug(" - {{link}} not processable and has no output version", "link", l)
			} else {
				p.Debug(" - {{link}} has no output version", "link", l)
			}
			state.AddMissing(l)
			continue
		}
		state.AddWaiting(l)
	}
	return state
}

func (p *elementReconcilation) forExtObjects(f func(log logging.Logger, object model.ExternalObject) error, set func(c *Controller, id TypeId) []string) error {
	exttypes := set(p.Controller(), p.Id().TypeId())
	for _, t := range exttypes {
		id := NewObjectId(t, p.GetNamespace(), p.GetName())
		log := p.WithValues("extid", id)
		_o, err := p.processingModel.ObjectBase().GetObject(database.NewObjectId(id.GetType(), id.GetNamespace(), id.GetName()))
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

func (p *elementReconcilation) updateStatus(status model.Status, message string, args ...any) error {
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
	p.Info(" updating status of external objects to {{newstatus}}: {{message}}", keys...)

	mod := func(log logging.Logger, o model.ExternalObject) error {
		return o.UpdateStatus(p.lctx, p.Objectbase(), p.Id(), update)
	}
	return p.forExtObjects(mod, UpdateObjects)
}

func (p *elementReconcilation) triggerChildren(release bool) {
	p.ni.lock.Lock()
	defer p.ni.lock.Unlock()
	// TODO: dependency check must be synchronized with this trigger

	id := p.eid
	p.Info("triggering children for {{element}} (checking {{amount}} elements in namespace)", "amount", len(p.ni.elements))
	for _, e := range p.ni.elements {
		if e.GetProcessingState() != nil {
			links := e.GetProcessingState().GetLinks()
			p.Debug("  elem {{child}} has target links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					p.Info("- trigger pending element {{waiting}} active in {{target-runid}}", "waiting", e.Id(), "target-runid", e.GetLock())
					p.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		} else if e.GetStatus() != model.STATUS_DELETED && e.GetCurrentState() != nil {
			links := e.GetCurrentState().GetObservedState().GetLinks()
			p.Debug("  elem {{child}} has current links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					p.Info("- trigger pending element {{waiting}}", "waiting", e.Id(), "target-runid", e.GetLock())
					p.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		}
	}
	if release {
		p._Element.SetProcessingState(nil)
	}
}

func (p *elementReconcilation) formalVersion(inputs model.Inputs) string {
	n := version.NewNode(p.Id().TypeId(), p.GetName(), p.GetTargetState().GetFormalObjectVersion())
	return p.composer.Compose(n, formalInputVersions(inputs)...)
}
