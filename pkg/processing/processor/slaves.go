package processor

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
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
