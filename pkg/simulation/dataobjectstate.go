package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type DataObjectState struct {
	InternalObject[database.DataObject] `json:",inline"`
}

var _ database.DataObjectState = (*DataObjectState)(nil)

func NewDataObjectState(name string) *DataObjectState {
	return newVersionedObject[DataObjectState](database.TYPE_DATAOBJECT_STATE, name)
}
