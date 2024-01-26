package support

import (
	"encoding/json"

	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

// State is default state implementation for any type which
// is json serializable.
type State[O any] struct {
	state O
}

func (s *State[O]) GetVersion() string {
	return utils.HashData(s.state)
}

func (e *State[O]) GetState() O {
	return e.state
}

func (s *State[O]) GetDescription() string {
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

type ResultState[O any] struct {
	State[O]
}

var _ model.ResultState = (*ResultState[any])(nil)
var _ json.Marshaler = (*ResultState[any])(nil)

func NewResultState[O any](o O) *ResultState[O] {
	return &ResultState[O]{State[O]{o}}
}

func (s *ResultState[O]) GetOutputVersion() string {
	return s.GetVersion()
}

func (s *ResultState[O]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.GetState())
}
