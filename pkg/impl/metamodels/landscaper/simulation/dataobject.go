package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodels/landscaper"
)

type DataObject struct {
	Object       `json:",inline"`
	Dependencies `json:",inline"`
}

var _ landscaper.DataObject = (*DataObject)(nil)

func NewDataObject(name string) *DataObject {
	return newVersionedObject[DataObject](landscaper.TYPE_DATAOBJECT, name)
}
