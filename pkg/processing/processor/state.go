package processor

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/model"
)

type ProcessingState interface {
	model.TargetState
	GetState() model.TargetState
}

////////////////////////////////////////////////////////////////////////////////

type tstate struct {
	element Element
}

var _ ProcessingState = (*tstate)(nil)

func NewTargetState(e Element) ProcessingState {
	return &tstate{
		element: e,
	}
}

func (s *tstate) GetState() model.TargetState {
	return s.element.GetObject().GetTargetState(s.element.GetPhase())
}

func (s *tstate) GetLinks() []ElementId {
	state := s.GetState()
	if state != nil {
		return state.GetLinks()
	}
	return nil
}

func (s *tstate) GetObjectVersion() string {
	state := s.GetState()
	if state != nil {
		return state.GetObjectVersion()
	}
	return ""
}

func (s *tstate) GetFormalObjectVersion() string {
	state := s.GetState()
	if state != nil {
		return state.GetFormalObjectVersion()
	}
	return ""
}

func (s *tstate) GetInputVersion(i model.Inputs) string {
	state := s.GetState()
	if state != nil {
		return state.GetInputVersion(i)
	}
	return ""
}
