package processor

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

type externalObjectReconciler struct {
	*reconciler
}

func newExternalObjectReconciler(c *Controller) *externalObjectReconciler {
	return &externalObjectReconciler{&reconciler{controller: c}}
}

func (r *externalObjectReconciler) Reconcile(_ pool.Pool, ctx pool.MessageContext, id database.ObjectId) pool.Status {
	return newExternalObjectReconcilation(r, ctx, id).Reconcile()
}

type externalObjectReconcilation struct {
	*externalObjectReconciler
	logging.Logger
	model.ExternalObject

	oid  database.ObjectId
	lctx model.Logging
}

func newExternalObjectReconcilation(r *externalObjectReconciler, lctx model.Logging, id database.ObjectId) *externalObjectReconcilation {
	oid := database.NewObjectIdFor(id)
	return &externalObjectReconcilation{
		externalObjectReconciler: r,
		Logger:                   lctx.Logger(REALM).WithValues("extid", oid),
		oid:                      oid,
		lctx:                     lctx,
	}
}
func (p *externalObjectReconcilation) Reconcile() pool.Status {
	t := p.MetaModel().GetPhaseFor(p.oid.GetType())
	if t == nil {
		return pool.StatusFailed(fmt.Errorf("external object type %q not configured", p.oid.GetType()))
	}

	// check for deletion
	err := p.handleExternalDeletion(*t)
	if err != nil || p.ExternalObject == nil {
		return pool.StatusCompleted(err)
	}

	// check for obsolete event and handle finalizer
	err = p.prepareExternalObject(*t)
	if err != nil {
		return pool.StatusCompleted(err)
	}

	// inventorize external object
	elem, err := p.Controller().processingModel.AssureElementObjectFor(p, p.ExternalObject)
	if err != nil {
		if IsNonTemporary(err) {
			return pool.StatusFailed(err)
		}
		return pool.StatusCompleted(err)
	}

	p.Logger = p.WithValues("element", elem.Id())
	runid := elem.GetLock()
	if runid != "" && !p.processingModel.MetaModel().GetExternalType(p.GetType()).IsForeignControlled() {
		p.Info("external object event for {{extid}} with active run {{runid}} -> delay trigger", "runid", runid)
		return pool.StatusCompleted(fmt.Errorf("run %s active -> delay trigger", runid))
	}

	p.Info("external object event for {{extid}} -> trigger element {{element}}")
	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}

func (p *externalObjectReconcilation) handleExternalDeletion(tid TypeId) error {
	o, err := p.Objectbase().GetObject(p.oid)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			if e := p.GetElement(NewElementIdForObject(tid, p.oid)); e != nil {
				p.Info("element {{element}} triggered for deleted external object {{exitid}}", "element", e.Id())
				p.Enqueue(CMD_EXT, e)
			} else {
				p.Info("ignoring change event for deleted external object {{extid}}")
			}
			return nil
		}
	}
	p.ExternalObject = o.(model.ExternalObject)
	return err
}

func (p *externalObjectReconcilation) prepareExternalObject(tid TypeId) error {
	eid := NewElementIdForObject(tid, p.ExternalObject)
	if p.GetElement(eid) == nil && p.IsDeleting() {
		ok, err := p.RemoveFinalizer(p.Objectbase(), FINALIZER)
		if err != nil {
			p.LogError(err, "cannot remove finalizer for deleting external object {{extid}} without assigned element")
			return err
		}
		if ok {
			p.Info("removed finalizer for deleting external object {{extid}} without assigned element")
		}
		p.Info("ignoring event for deleting external object {{extid}} without assigned element")
		return nil
	}
	ok, err := p.AddFinalizer(p.Objectbase(), FINALIZER)
	if err != nil {
		p.LogError(err, "cannot add finalizer for external object {{extid}}")
		return err
	}
	if ok {
		p.Info("added finalizer for external object {{extid}}")
	}
	return nil
}
