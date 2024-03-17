package processing

import (
	"encoding/json"

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

func NewState[O any](o O) *State[O] {
	return &State[O]{o}
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
