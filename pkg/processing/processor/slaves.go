package processor

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/logging"
)

type SlaveManagement struct {
	log  logging.Logger
	p    *Controller
	ni   *namespaceInfo
	elem _Element
}

var _ model.SlaveManagement = (*SlaveManagement)(nil)

func newSlaveManagement(r Reconcilation, ni *namespaceInfo, elem _Element) model.SlaveManagement {
	return &SlaveManagement{
		log:  r,
		p:    r.Controller(),
		ni:   ni,
		elem: elem,
	}
}

func (s *SlaveManagement) AssureSlaves(check model.SlaveCheckFunction, update model.SlaveUpdateFunction, eids ...ElementId) error {
	for _, eid := range eids {
		if !s.p.processingModel.MetaModel().HasElementType(eid.TypeId()) {
			return fmt.Errorf("unknown element type %q for slave of %q", eid.TypeId(), s.elem.Id())
		}
	}
	return s.ni.assureSlaves(s.log, s.p, check, update, s.elem.GetLock(), eids...)
}

func (s *SlaveManagement) ObjectBase() objectbase.Objectbase {
	return s.p.processingModel.ObjectBase()
}

func (s *SlaveManagement) MarkForDeletion(eids ...ElementId) error {
	for _, eid := range eids {
		e := s.ni._GetElement(eid)
		if e == nil {
			continue
		}
		if !e.IsMarkedForDeletion() {
			s.log.Info("mark slave {{slave}} for deletion", "slave", eid)
			_, _, leafs, err := e.MarkForDeletion(s.p.processingModel)
			if err != nil {
				return err
			}
			s.log.Info("triggering leaf phases {{phases}} for deletion", "phases", leafs)
			for _, phase := range leafs {
				id := NewElementIdForPhase(e, phase)
				s.log.Debug(" - triggering {{leaf}}", "leaf", id)
				s.p.EnqueueKey(CMD_ELEM, id)
			}
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *SlaveManagement) AssureExternal(update model.ExternalUpdateFunction, extid database.ObjectId) (bool, model.ExternalObject, error) {
	if !s.p.processingModel.MetaModel().IsExternalType(extid.GetType()) {
		return false, nil, fmt.Errorf("unknown external type %q for external slave  %q", extid.GetType(), s.elem.Id())
	}

	// first, check existing objects
	var modobj model.ExternalObject

	log := s.log.WithValues("extid", extid)
	ob := s.ObjectBase()
	log.Info("checking external slave object {{extid}}")
	_o, err := ob.GetObject(extid)
	if errors.Is(err, database.ErrNotExist) {
		log.Info("external slave object {{extid}} not found -> create it")
	}
	_o, err = ob.CreateObject(extid)
	if err != nil {
		log.Info("cannot create external slave object {{extid}}")
	}

	// second, update/create required objects
	modobj = _o.(model.ExternalObject)
	return update(ob, extid, modobj)
}
