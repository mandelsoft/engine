package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodels/landscaper"
)

type DataObjectState struct {
	InternalObject[landscaper.DataObject] `json:",inline"`
}

var _ landscaper.DataObjectState = (*DataObjectState)(nil)

func NewDataObjectState(name string) *DataObjectState {
	return newVersionedObject[DataObjectState](landscaper.TYPE_DATAOBJECT_STATE, name)
}
