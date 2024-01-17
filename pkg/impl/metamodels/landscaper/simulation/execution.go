package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodels/landscaper"
)

type Execution struct {
	Object
	Dependencies
}

var _ landscaper.Execution = (*Execution)(nil)

func NewExecution(name string) *Execution {
	return newVersionedObject[Execution](landscaper.TYPE_EXECUTION, name)
}
