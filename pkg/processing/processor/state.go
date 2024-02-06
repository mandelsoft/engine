package processor

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/model"
)

type CurrentState interface {
	model.CurrentState
	GetState() model.CurrentState
}

type TargetState interface {
	model.TargetState
	GetState() model.TargetState
}

////////////////////////////////////////////////////////////////////////////////

type cstate struct {
	element Element
}

var _ CurrentState = (*cstate)(nil)

func NewCurrentState(e Element) CurrentState {
	return &cstate{
		element: e,
	}
}

func (s *cstate) GetState() model.CurrentState {
	return s.element.GetObject().GetCurrentState(s.element.GetPhase())
}

func (s *cstate) GetLinks() []ElementId {
	state := s.GetState()
	if state != nil {
		return state.GetLinks()
	}
	return nil
}

func (s *cstate) GetInputVersion() string {
	state := s.GetState()
	if state != nil {
		return state.GetInputVersion()
	}
	return ""
}

func (s *cstate) GetObjectVersion() string {
	state := s.GetState()
	if state != nil {
		return state.GetObjectVersion()
	}
	return ""
}

func (s *cstate) GetOutputVersion() string {
	state := s.GetState()
	if state != nil {
		return state.GetOutputVersion()
	}
	return ""
}

func (s *cstate) GetOutput() model.OutputState {
	state := s.GetState()
	if state != nil {
		return state.GetOutput()
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type tstate struct {
	element Element
}

var _ TargetState = (*tstate)(nil)

func NewTargetState(e Element) TargetState {
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

func (s *tstate) GetInputVersion(i model.Inputs) string {
	state := s.GetState()
	if state != nil {
		return state.GetInputVersion(i)
	}
	return ""
}