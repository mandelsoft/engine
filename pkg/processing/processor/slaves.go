package processor

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/logging"
)

type SlaveManagement struct {
	log  logging.Logger
	p    *Processor
	ni   *namespaceInfo
	elem _Element
}

var _ model.SlaveManagement = (*SlaveManagement)(nil)

func newSlaveManagement(log logging.Logger, p *Processor, ni *namespaceInfo, elem _Element) model.SlaveManagement {
	return &SlaveManagement{
		log:  log,
		p:    p,
		ni:   ni,
		elem: elem,
	}
}

func (s *SlaveManagement) AssureSlaves(check model.SlaveCheckFunction, update model.SlaveUpdateFunction, eids ...ElementId) error {
	for _, eid := range eids {
		if !s.p.processingModel.MetaModel().HasElementType(eid.TypeId()) {
			return fmt.Errorf("unknown element type %q for slave od %q", eid, s.elem.Id())
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
