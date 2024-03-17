package support

import (
	"encoding/json"

	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/processing/model"
)

type ExternalState[O any] struct {
	processing.State[O]
}

var _ model.ExternalState = (*ExternalState[any])(nil)

func NewExternalState[O any](o O) *ExternalState[O] {
	return &ExternalState[O]{*processing.NewState(o)}
}

////////////////////////////////////////////////////////////////////////////////

type OutputState[O any] struct {
	formal string
	processing.State[O]
}

var _ model.OutputState = (*OutputState[any])(nil)
var _ json.Marshaler = (*OutputState[any])(nil)

func NewOutputState[O any](formal string, o O) *OutputState[O] {
	return &OutputState[O]{formal, *processing.NewState(o)}
}

func (s *OutputState[O]) GetFormalVersion() string {
	return s.formal
}

func (s *OutputState[O]) GetOutputVersion() string {
	return s.GetVersion()
}

func (s *OutputState[O]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.GetState())
}
