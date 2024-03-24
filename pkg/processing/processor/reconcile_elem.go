package processor

import (
	"fmt"
	"time"

	"github.com/mandelsoft/engine/pkg/locks"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

type elementReconciler struct {
	*reconciler
	busy *locks.ElementLocks[ElementId]
}

func newElementReconciler(c *Controller) *elementReconciler {
	return &elementReconciler{reconciler: &reconciler{controller: c}, busy: locks.NewElementLocks[ElementId]()}
}

func (r *elementReconciler) Command(_ pool.Pool, ctx pool.MessageContext, command pool.Command) pool.Status {
	// ctx = logging.ExcludeFromMessageContext[logging.Realm](ctx)
	ctx = ctx.WithContext(REALM)
	cmd, _, id := DecodeCommand(command)
	if id != nil {
		if r.Controller().delay > 0 {
			time.Sleep(r.Controller().delay)
		}
		// there are two queue keys for elements, so we have to synchronize manually
		// to avoid parallel reconcilation of the same element.
		if !r.busy.TryLock(*id) {
			return pool.StatusCompleted(fmt.Errorf("element busy"))
		}
		defer r.busy.Unlock(*id)
		switch cmd {
		case CMD_EXT:
			return newElementExtReconcilation(r, ctx, *id).Reconcile()
		case CMD_ELEM:
			return newElementRunReconcilation(r, ctx, *id).Reconcile()
		}
	}
	return pool.StatusFailed(fmt.Errorf("invalid element command %q", command))
}

type elementReconcilation struct {
	*elementReconciler
	logging.Logger
	_Element

	eid  ElementId
	lctx model.Logging
	bctx model.Logging
}

func newElementReconcilation(r *elementReconciler, bctx model.Logging, eid ElementId) *elementReconcilation {
	lctx := bctx.WithValues("namespace", eid.GetNamespace(), "element", eid).WithName(eid.String())
	log := bctx.Logger()
	elem := r.processingModel._GetElement(eid)

	return &elementReconcilation{
		elementReconciler: r,
		Logger:            log,
		_Element:          elem,

		lctx: lctx,
		bctx: bctx,
		eid:  eid,
	}
}

func (r *elementReconcilation) LoggingContext() model.Logging {
	return r.LoggingContext()
}

func (r *elementReconcilation) isReTriggerable(e _Element, ext ...string) bool {
	if e.GetLock() != "" {
		if e.IsMarkedForDeletion() {
			return true
		}
		var ttyp *string
		if len(ext) == 0 {
			ttyp = r.processingModel.MetaModel().GetTriggerTypeForElementType(e.Id().TypeId())
		} else {
			ttyp = &ext[0]
		}
		if ttyp != nil {
			if r.processingModel.MetaModel().IsForeignControlled(*ttyp) {
				if isProcessable(e) {
					return true
				}
			}
		}
	}
	return false
}

func (r *elementReconcilation) initiateNewRun() pool.Status {
	r.Info("trying to initiate new run for {{element}}")
	ni := r.getNamespaceInfo(r.GetNamespace())
	rid, err := ni.LockGraph(r, r._Element)
	if err == nil {
		if rid != nil {
			r.Info("starting run {{runid}}", "runid", *rid)
			r.EnqueueKey(CMD_ELEM, r.eid)
		} else {
			err = fmt.Errorf("delay initiation of new run")
		}
	}
	return pool.StatusCompleted(err)
}
