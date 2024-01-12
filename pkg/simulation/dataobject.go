package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type DataObject struct {
	Object       `json:",inline"`
	Dependencies `json:",inline"`
}

var _ database.DataObject = (*DataObject)(nil)

func NewDataObject(name string) *DataObject {
	return newVersionedObject[DataObject](database.TYPE_DATAOBJECT, name)
}
