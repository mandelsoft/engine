package processor

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/errors"
	"github.com/mandelsoft/goutils/general"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
)

type elementExtReconcilation struct {
	*elementReconcilation
}

func newElementExtReconcilation(r *elementReconciler, bctx model.Logging, eid ElementId) *elementExtReconcilation {
	return &elementExtReconcilation{newElementReconcilation(r, bctx, eid)}
}

func (r *elementExtReconcilation) Reconcile() pool.Status {
	if r._Element != nil && r.GetStatus() == model.STATUS_DELETED {
		r.Debug("skip deleted element {{element}}")
		return pool.StatusCompleted()
	}

	if r._Element == nil {
		var status pool.Status
		status = r.handleNew()
		if r._Element == nil {
			return status
		}
	}

	runid := r.GetLock()
	if runid == "" || r.isReTriggerable(r._Element) || r.IsMarkedForDeletion() {
		if runid == "" {
			r.Debug("new external change for {{element}} (deletion requested: {{marked}})", "marked", r.IsMarkedForDeletion())
		} else {
			r.Debug("retriggering external change for {{element}} (deletion requested: {{marked}})", "marked", r.IsMarkedForDeletion())
		}
		return r.handleExternalChange()
	}
	r.Debug("skip external object trigger for {{element}}")
	return pool.StatusCompleted()
}

func (r *elementExtReconcilation) handleNew() pool.Status {
	r.Info("processing unknown element {{element}}")

	_i, err := r.Objectbase().GetObject(r.eid)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return pool.StatusCompleted(err)
		}
		r.Info("internal object not found for {{element}}")

		_, ext, err := r.getTriggeringExternalObject(r.eid)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		if ext == nil {
			r.Info("no triggering object found -> obsolete event -> don't create new element")
			return pool.StatusCompleted()
		}
		if r.isDeleting(ext) {
			r.Info("external object is deleting -> don't create new element")
			return pool.StatusCompleted()
		}

		_i, err = r.processingModel.ObjectBase().CreateObject(r.eid)
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}
	i := _i.(model.InternalObject)

	ni, err := r.Controller().processingModel.AssureNamespace(r.Logger, i.GetNamespace(), true)
	if err != nil {
		return pool.StatusCompleted(err)
	}

	r._Element = ni._AddElement(i, r.GetPhase())
	return pool.StatusCompleted()
}

func (r *elementExtReconcilation) handleExternalChange() pool.Status {
	r.Info("processing external element trigger for {{element}} with status {{status}}", "status", r.GetStatus())

	trigger := r.processingModel.MetaModel().GetTriggerTypeForElementType(r.Id().TypeId())
	if trigger == nil {
		r.Info("no triggering types for {{element}}")
		return pool.StatusCompleted()
	}

	if !isExtTriggerable(r._Element) {
		if !r.isReTriggerable(r._Element, *trigger) {
			if !r.IsMarkedForDeletion() {
				r.Info("state for element in status {{status}} is already assigned", "status", r.GetStatus())
				return pool.StatusCompleted()
			}
		}
		r.Info("state for element in status {{status}} is already assigned but retriggerable", "status", r.GetStatus())
	}

	r.Info("checking state of external objects for element {{element}}", "exttypes", trigger)
	var changed *string
	deleting := false
	cur := r.GetCurrentState().GetObservedState().GetObjectVersion()

	id := database.NewObjectId(*trigger, r.GetNamespace(), r.GetName())
	r.Logger = r.WithValues("extid", id)
	_o, err := r.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			r.LogError(err, "cannot get external object {{extid}}")
			return pool.StatusCompleted(fmt.Errorf("cannot get external object %s: %w", id, err))
		}
		r.Info("external object {{extid}} not found -> ignore state")
		return pool.StatusCompleted()
	}

	o := _o.(model.ExternalObject)
	if o.IsDeleting() {
		r.Info("external object {{extid}} requests deletion")
		deleting = true
	}

	// give the internal object the chance to modify the actual external state
	ov := o.GetState().GetVersion()
	es := r.GetExternalState(o)
	v := es.GetVersion()
	if ov != v {
		r.Info("state of external object {{extid}} adjusted from {{objectversion}} to {{version}}", "objectversion", ov, "version", v)
		r.Debug("external state: {{state}}", "state", general.DescribeObject(es))
	}
	if v == cur {
		r.Info("state of external object {{extid}} not changed ({{version}})", "version", v)
	} else {
		changed = trigger
		r.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
	}

	if changed == nil && !deleting {
		r.Info("no external object state change found for {{element}}")
		return pool.StatusCompleted()
	}

	var leafs []Phase
	var phases []Phase
	if deleting {
		var ok bool
		var err error
		r.Info("element {{element}} should be deleted")
		ok, phases, leafs, err = r.MarkForDeletion(r.processingModel)
		if err != nil {
			r.LogError(err, "cannot mark all dependent phases ({phases}}) of {{element}} as deleting", "phases", phases)
			return pool.StatusCompleted(err)
		}
		if ok {
			r.Info("marked dependent phases ({{phases}}) of {{element}} as deleting", "phases", phases)
		}
	}

	if changed != nil && r.isReTriggerable(r._Element, *changed) {
		r.Info("retrigger state change of external object {{extid}} for {{element}}")
		r.EnqueueKey(CMD_ELEM, r.eid)
		return pool.StatusCompleted()
	}

	if changed != nil {
		return r.initiateNewRun()
	}

	r.Info("triggering leaf phases {{phases}} for deletion", "phases", leafs)
	for _, phase := range leafs {
		id := NewElementIdForPhase(r.eid, phase)
		r.Debug(" - triggering {{leaf}}", "leaf", id)
		r.EnqueueKey(CMD_ELEM, id)
	}
	return pool.StatusCompleted()
}
