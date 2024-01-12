package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Execution struct {
	Object
	Dependencies
}

var _ database.Execution = (*Execution)(nil)

func NewExecution(name string) *Execution {
	return newVersionedObject[Execution](database.TYPE_EXECUTION, name)
}
