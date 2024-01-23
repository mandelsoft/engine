package processing

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
)

type State interface {
	AddLink(ElementId)

	GetLinks() []ElementId
	GetVersion() string
}

type objStateFunc func(o common.InternalObject, phase model.Phase) common.State

func ObjectState(o common.InternalObject, phase model.Phase) common.State {
	return o.GetState(phase)
}

func ObjectTargetState(o common.InternalObject, phase model.Phase) common.State {
	return o.GetTargetState(phase)
}

type state struct {
	state   objStateFunc
	element Element
	links   []ElementId
}

var _ State = (*state)(nil)

func NewState(e Element, s objStateFunc) State {
	return &state{
		state:   s,
		element: e,
	}
}

func (s *state) AddLink(id ElementId) {
	s.links = append(s.links, id)
}

func (s *state) GetLinks() []ElementId {
	return slices.Clone(s.links)
}

func (s *state) GetVersion() string {
	c := s.state(s.element.GetObject(), s.element.Id().Phase())
	if c == nil {
		return ""
	}
	return c.GetVersion()
}
