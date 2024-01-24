package processing

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
)

type CurrentState interface {
	model.CurrentState
}

type TargetState interface {
	model.TargetState
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

func (s *cstate) objectState() model.CurrentState {
	return s.element.GetObject().GetCurrentState(s.element.GetPhase())
}

func (s *cstate) GetLinks() []ElementId {
	state := s.objectState()
	if state != nil {
		return state.GetLinks()
	}
	return nil
}

func (s *cstate) GetInputVersion() string {
	state := s.objectState()
	if state != nil {
		return state.GetInputVersion()
	}
	return ""
}

func (s *cstate) GetObjectVersion() string {
	state := s.objectState()
	if state != nil {
		return state.GetObjectVersion()
	}
	return ""
}

func (s *cstate) GetOutputVersion() string {
	state := s.objectState()
	if state != nil {
		return state.GetOutputVersion()
	}
	return ""
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

func (s *tstate) objectState() model.TargetState {
	return s.element.GetObject().GetTargetState(s.element.GetPhase())
}

func (s *tstate) GetLinks() []ElementId {
	state := s.objectState()
	if state != nil {
		return state.GetLinks()
	}
	return nil
}

func (s *tstate) GetObjectVersion() string {
	state := s.objectState()
	if state != nil {
		return state.GetObjectVersion()
	}
	return ""
}

func (s *tstate) GetInputVersion(i model.Inputs) string {
	state := s.objectState()
	if state != nil {
		return state.GetInputVersion(i)
	}
	return ""
}
