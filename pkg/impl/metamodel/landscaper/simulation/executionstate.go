package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodel/landscaper"
)

type ExecutionState struct {
	InternalObject[landscaper.Execution] `json:",inline"`
}

var _ landscaper.ExecutionState = (*ExecutionState)(nil)

func NewExecutionStatee(name string) *ExecutionState {
	return newVersionedObject[ExecutionState](landscaper.TYPE_EXECUTION_STATE, name)
}
