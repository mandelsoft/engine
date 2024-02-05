package support

import (
	"encoding/json"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Describable interface {
	GetDescription() string
}

// State is default state implementation for any type which
// is json serializable.
type State[O any] struct {
	state O
}

var _ Describable = (*State[any])(nil)

func (s *State[O]) GetVersion() string {
	return utils.HashData(s.state)
}

func (e *State[O]) GetState() O {
	return e.state
}

func (s *State[O]) GetDescription() string {
	if d, ok := utils.TryCast[Describable](s.state); ok {
		return d.GetDescription()
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		panic(err)
	}
	return string(data)
}

////////////////////////////////////////////////////////////////////////////////

type ExternalState[O any] struct {
	State[O]
}

var _ model.ExternalState = (*ExternalState[any])(nil)

func NewExternalState[O any](o O) *ExternalState[O] {
	return &ExternalState[O]{State[O]{o}}
}

////////////////////////////////////////////////////////////////////////////////

type OutputState[O any] struct {
	State[O]
}

var _ model.OutputState = (*OutputState[any])(nil)
var _ json.Marshaler = (*OutputState[any])(nil)

func NewOutputState[O any](o O) *OutputState[O] {
	return &OutputState[O]{State[O]{o}}
}

func (s *OutputState[O]) GetOutputVersion() string {
	return s.GetVersion()
}

func (s *OutputState[O]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.GetState())
}
