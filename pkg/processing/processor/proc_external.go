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

func (p *Processor) processExternalObject(log logging.Logger, id database.ObjectId) pool.Status {
	oid := NewObjectIdFor(id)
	m := p.processingModel
	log = log.WithName(oid.String()).WithValues("extid", oid)

	t := m.MetaModel().GetPhaseFor(id.GetType())
	if t == nil {
		return pool.StatusFailed(fmt.Errorf("external object type %q not configured", id.GetType()))
	}

	// check for deletion
	_o, err := p.handleExternalDeletion(log, id, *t)
	if err != nil || _o == nil {
		return pool.StatusCompleted(err)
	}

	// check for obsolete event and handle finalizer
	eid := NewElementIdForObject(*t, _o)
	o, err := p.prepareExternalObject(log, eid, _o.(model.ExternalObject))
	if err != nil || o == nil {
		return pool.StatusCompleted(err)
	}

	// inventorize external object
	elem, err := p.processingModel.AssureElementObjectFor(log, o)
	if err != nil {
		if IsNonTemporary(err) {
			return pool.StatusFailed(err)
		}
		return pool.StatusCompleted(err)
	}

	log = log.WithValues("element", elem.Id())
	runid := elem.GetLock()
	if runid != "" && !p.processingModel.MetaModel().GetExternalType(id.GetType()).IsForeignControlled() {
		log.Info("external object event for {{extid}} with active run {{runid}} -> delay trigger", "runid", runid)
		return pool.StatusCompleted(fmt.Errorf("run %s active -> delay trigger", runid))
	}

	log.Info("external object event for {{extid}} -> trigger element {{element}}")
	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}

func (p *Processor) handleExternalDeletion(log logging.Logger, id database.ObjectId, t TypeId) (model.Object, error) {
	o, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			if e := p.GetElement(NewElementIdForObject(t, id)); e != nil {
				log.Info("element {{element}} triggered for deleted external object {{exitid}}", "element", e.Id())
				p.Enqueue(CMD_EXT, e)
				return nil, nil
			}
		}
		log.Info("ignoring deleted external object {{exitid}}")
	}
	return o, err
}

func (p *Processor) prepareExternalObject(log logging.Logger, eid ElementId, o model.ExternalObject) (model.ExternalObject, error) {
	m := p.processingModel

	if m.getElement(eid) == nil && o.IsDeleting() {
		ok, err := o.RemoveFinalizer(m.ob, FINALIZER)
		if err != nil {
			log.LogError(err, "cannot remove finalizer for deleting external object {{extid}} without assigned element")
			return nil, err
		}
		if ok {
			log.Info("removed finalizer for deleting external object {{extid}} without assigned element")
		}
		log.Info("ignoring event for deleting external object {{extid}} without assigned element")
		return nil, nil
	}
	ok, err := o.AddFinalizer(m.ob, FINALIZER)
	if err != nil {
		log.LogError(err, "cannot add finalizer for external object {{extid}}")
		return nil, err
	}
	if ok {
		log.Info("added finalizer for deleting external object {{extid}}")
	}
	return o, nil
}
