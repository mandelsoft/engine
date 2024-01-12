package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type ExecutionState struct {
	InternalObject[database.Execution] `json:",inline"`
}

var _ database.ExecutionState = (*ExecutionState)(nil)

func NewExecutionStatee(name string) *ExecutionState {
	return newVersionedObject[ExecutionState](database.TYPE_EXECUTION_STATE, name)
}
