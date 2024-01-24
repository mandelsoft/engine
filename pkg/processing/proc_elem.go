package processing

import (
	"errors"
	"fmt"
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processElement(lctx logging.Context, cmd string, id ElementId) pool.Status {
	lctx = lctx.WithContext(logging.NewAttribute("namespace", id.Namespace()), logging.NewAttribute("element", id))

	elem := p.GetElement(id)
	if elem == nil {
		if cmd != CMD_EXT {
			return pool.StatusFailed(fmt.Errorf("unknown element %q", id))
		}
		var status pool.Status
		elem, status = p.handleNew(lctx, id)
		if elem == nil {
			return status
		}
	}

	runid := elem.GetLock()
	if runid == "" {
		if cmd == CMD_EXT {
			return p.handleExternalChange(lctx, elem)
		}
	} else {
		return p.handleRun(lctx.WithContext(logging.NewAttribute("runid", runid)), elem)
	}
	return pool.StatusCompleted()
}

func (p *Processor) handleNew(lctx logging.Context, id ElementId) (Element, pool.Status) {

	_i, err := p.ob.GetObject(id.DBId())
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return nil, pool.StatusCompleted(err)
		}
		_i, err = p.ob.CreateObject(id.ObjectId())
		if err != nil {
			return nil, pool.StatusCompleted(err)
		}
	}
	i := _i.(model.InternalObject)

	ni, err := p.AssureNamespace(i.GetNamespace(), true)
	if err != nil {
		return nil, pool.StatusCompleted(err)
	}
	ni.lock.Lock()
	defer ni.lock.Unlock()

	return ni.AddElement(i, id.Phase()), pool.StatusCompleted()
}

func (p *Processor) handleExternalChange(lctx logging.Context, e Element) pool.Status {
	log := lctx.Logger()
	types := p.mm.GetTriggeringTypesForElementType(e.Id().TypeId())
	if len(types) > 0 {
		log.Info("checking state of external objects for element {{element}}")
		changed := false
		for _, t := range types {
			id := database.NewObjectId(t, e.GetNamespace(), e.GetName())
			log := log.WithValues("extid", id)
			_o, err := p.ob.GetObject(id)
			if err != nil {
				log.Error("cannot get external object {{extid}}")
				continue
			}

			cur := e.GetCurrentState().GetObjectVersion()
			o := _o.(model.ExternalObject)

			v := o.GetState().GetVersion()
			if v == cur {
				log.Info("state of external object {{extid}} not changed")
			} else {
				changed = true
				log.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
			}
		}
		if changed {
			log.Info("trying to initiate new run for {{element}}")

			rid, err := p.lockGraph(lctx, e)
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

func (p *Processor) handleRun(lctx logging.Context, e Element) pool.Status {
	log := lctx.Logger()
	ni := p.GetNamespace(e.GetNamespace())

	var missing, waiting []ElementId
	var inputs model.Inputs

	var ext []model.ObjectId
	for _, t := range p.mm.GetTriggeringTypesForElementType(e.Id().TypeId()) {
		ext = append(ext, common.NewObjectId(t, e.GetNamespace(), e.GetName()))
	}

	if e.GetTargetState() == nil {
		// check current dependencies (target state not yet fixed)
		log.Info("checking current links")
		missing, waiting, inputs = p.checkReady(ni, e.GetCurrentState().GetLinks())

		if p.notifyWaitingState(log, e, missing, waiting, inputs) {
			return pool.StatusCompleted(fmt.Errorf("still waiting for predecessors"))
		}
		// fix target state by transferring the current external state to the internal object
		err := p.hardenTargetState(log, e)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		// checking target dependencies after fixing the target state
		log.Info("checking target links and get actual inputs")
		missing, waiting, inputs = p.checkReady(ni, e.GetObject().GetTargetState(e.GetPhase()).GetLinks())
		if p.notifyWaitingState(log, e, missing, waiting, inputs) {
			return pool.StatusCompleted(fmt.Errorf("still waiting for effective predecessors"))
		}

		target := e.GetObject().GetTargetState(e.GetPhase())

		// check effective version for required phase processing.
		if target.GetInputVersion(inputs) == e.GetCurrentState().GetInputVersion() &&
			target.GetObjectVersion() == e.GetCurrentState().GetObjectVersion() {
			log.Info("effective version unchanged -> skip processing of phase")
			err = p.notifyCompletedState(log, ni, e, "no processing required", nil)
			return pool.StatusCompleted(err)
		}
		// mark element to be ready by setting the elements target state to the target state of the internal
		// object for the actual phase
		e.SetTargetState(target)
	}

	// now we can process the phase
	status := e.GetObject().Process(p.ob, model.Request{
		LogContext: lctx,
		External:   ext,
		Element:    e,
		Inputs:     inputs,
	})

	if status.Error != nil {
		if status.Status == common.STATUS_FAILED {
			// non-recoverable error, wait for new change in external object state
			log.Error("processing provides non recoverable error", "error", status.Error)
			p.updateStatus(e, "Failed", status.Error.Error())
			return pool.StatusFailed(status.Error)
		}
		log.Error("processing provides error", "error", status.Error)
		p.updateStatus(e, "Processing", status.Error.Error())
		return pool.StatusCompleted(status.Error)
	} else {
		// if no error is provided, check for requested object creation.
		// an execution might provide new internal objects.
		// this objects MUST already be configured with the required links,
		// especially the one to the current element.
		// If processed, those object MUST create the matching external object.
		for _, c := range status.Creation {
			if p.mm.GetInternalType(c.Internal.GetType()) == nil {
				log.Error("skipping creation of requested object for unknown internal type {{type}}", "type", c.Internal.GetType())
				continue
			}
			n := ni.AddElement(c.Internal, c.Phase)
			ok, err := n.TryLock(p.ob, e.GetLock())
			if !ok {
				panic(fmt.Sprintf("cannot lock new element: %s", err))
			}
		}
		if status.Status == common.STATUS_COMPLETED {
			err := p.notifyCompletedState(log, ni, e, "processing completed", status.InternalState, CalcEffectiveVersion(inputs, e.GetTargetState().GetObjectVersion()))
			if err != nil {
				return pool.StatusCompleted(err)
			}
		}
	}
	return pool.StatusCompleted()
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) notifyCompletedState(log logging.Logger, ni *NamespaceInfo, e Element, msg string, args ...interface{}) error {
	// TODO: handler cleanup of target state
	_, err := e.ClearLock(p.ob, e.GetLock())
	if err != nil {
		log.Error("cannot unlock element {{element}}", "error", err)
		return err
	}
	log.Info("processing of element {{element}} completed")
	p.updateStatus(e, "Completed", msg, append(args, model.RunId(""))...)
	p.triggerChildren(ni, e, true)
	return nil
}

func (p *Processor) notifyWaitingState(log logging.Logger, e Element, missing, waiting []ElementId, inputs model.Inputs) bool {
	if len(waiting) > 0 || len(missing) > 0 {
		var keys []interface{}
		if len(missing) > 0 {
			keys = append(keys, "missing", utils.Join(missing))
		}
		if len(waiting) > 0 {
			keys = append(keys, "waiting", utils.Join(waiting))
		}
		if len(inputs) > 0 {
			keys = append(keys, "found", utils.Join(utils.MapKeys(inputs)))
		}
		log.Info("inputs not ready", keys...)
		if len(missing) > 0 {
			p.updateStatus(e, "Waiting", fmt.Sprintf("unresolved dependencies %s", utils.Join(missing)), nil, e.GetLock())
		} else {
			p.updateStatus(e, "Pending", fmt.Sprintf("waiting for %s", utils.Join(waiting)), e.GetLock())
		}
		return true
	}
	return false
}

func (p *Processor) hardenTargetState(log logging.Logger, e Element) error {
	// determie potential external objects
	var ext []model.ObjectId
	for _, t := range p.mm.GetTriggeringTypesForElementType(e.Id().TypeId()) {
		ext = append(ext, common.NewObjectId(t, e.GetNamespace(), e.GetName()))
	}

	if e.GetObject().GetTargetState(e.GetPhase()) == nil {
		log.Info("target state for internal object of {{element}} already set for actual phase")
	} else {
		if len(ext) > 0 {
			log.Info("setting state of external objects for element {{element}}")
			for _, id := range ext {
				log := log.WithValues("extid", id)
				_o, err := p.ob.GetObject(database.NewObjectId(id.Type(), id.Namespace(), id.Name()))
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
				err = o.UpdateStatus(p.ob, e.Id(), common.StatusUpdate{
					RunId:           utils.Pointer(e.GetLock()),
					DetectedVersion: &v,
					ObservedVersion: nil,
					Status:          utils.Pointer("Preparing"),
					Message:         utils.Pointer(""),
					InternalState:   nil,
				})
				if err != nil {
					log.Error("cannot update status for external object {{extid}}", "error", err)
					return err
				}
				err = e.GetObject().SetExternalState(p.ob, e.GetPhase(), o.GetType(), state)
				if err != nil {
					log.Error("cannot update external state for internal object from {{extid}}", "error", err)
					return err
				}
				log.Info("internal object fixes state of {{extid}} to {{version}}", "version", v)
			}
		}
	}
	return nil
}

func (p *Processor) lockGraph(lctx logging.Context, elem Element) (*model.RunId, error) {
	id := model.NewRunId()
	ns := p.GetNamespace(elem.GetNamespace())

	if !ns.lock.TryLock() {
		return nil, nil
	}
	defer ns.lock.Unlock()

	log := lctx.Logger(logging.NewAttribute("runid", id))
	ok, err := ns.namespace.TryLock(p.ob, id)
	if err != nil {
		log.Info("locking  namespace {{namespace}} for new runid {{runid}} failed", "error", err)
		return nil, err
	}
	if !ok {
		log.Info("cannot lock namespace {{namespace}} for already locked for {{current}}", "current", ns.namespace.GetLock())
		return nil, nil
	}
	log.Info("namespace {{namespace}} locked for new runid {{runid}}")
	defer func() {
		err := ns.clearLocks(log, p)
		if err != nil {
			log.Error("cannot clear namespace lock for {{namespace}} -> requeue", "error", err)
			p.EnqueueNamespace(ns.GetNamespaceName())
		}
	}()

	elems := map[ElementId]Element{}
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

func (p *Processor) getChildren(ns *NamespaceInfo, elem Element) []Element {
	var r []Element
	id := elem.Id()
	for _, e := range ns.elements {
		state := e.GetCurrentState()
		if state != nil {
			if slices.Contains(state.GetLinks(), id) {
				r = append(r, e)
			}

		}
	}
	return r
}

func (p *Processor) _tryLockGraph(log logging.Logger, ns *NamespaceInfo, elem Element, elems map[ElementId]Element) (bool, error) {
	if elems[elem.Id()] == nil {
		cur := elem.GetLock()
		if cur != "" {
			return false, nil
		}
		elems[elem.Id()] = elem

		for _, d := range p.getChildren(ns, elem) {
			ok, err := p._tryLockGraph(log, ns, d, elems)
			if !ok || err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (p *Processor) _lockGraph(log logging.Logger, ns *NamespaceInfo, elems map[ElementId]Element, id model.RunId) (bool, error) {
	var ok bool
	var err error

	ns.pendingElements = map[ElementId]Element{}

	for _, elem := range elems {
		log.Debug("locking {{nestedelem}}", "nestedelem", elem.Id())
		ok, err = elem.TryLock(p.ob, id)
		if !ok || err != nil {
			log.Debug("locking failed for {{nestedelem}}", "nestedelem", elem.Id(), "error", err)
			return false, err
		}
		ns.pendingElements[elem.Id()] = elem

	}
	ns.pendingElements = nil
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) checkReady(ni *NamespaceInfo, links []ElementId) ([]ElementId, []ElementId, model.Inputs) {
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
			if t.GetLock() == "" {
				inputs[l] = t.GetCurrentState()
			} else {
				waiting = append(waiting, l)
			}
		}
	}
	return missing, waiting, inputs
}