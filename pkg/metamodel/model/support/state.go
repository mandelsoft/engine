package support

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/gowebpki/jcs"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
)

type ExternalState[O any] struct {
	state O
}

var _ model.ExternalState = (*ExternalState[any])(nil)

func NewExternalState[O any](o O) *ExternalState[O] {
	return &ExternalState[O]{o}
}

func (e *ExternalState[O]) GetVersion() string {
	data, err := json.Marshal(e.state)
	if err != nil {
		panic(err)
	}
	data, err = jcs.Transform(data)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (e *ExternalState[O]) GetState() O {
	return e.state
}
