package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
)

//
// Serializable Ids to be used in db object types
//

type ObjectId struct {
	ObjType        string `json:"type"`
	database.Named `json:",inline"`
}

var _ database.ObjectId = ObjectId{}

func NewObjectId(typ, namespace, name string) ObjectId {
	return ObjectId{
		ObjType: typ,
		Named: database.Named{
			Name:      name,
			Namespace: namespace,
		},
	}
}
func NewObjectIdFor(id database.ObjectId) ObjectId {
	return ObjectId{
		ObjType: id.GetType(),
		Named: database.Named{
			Name:      id.GetName(),
			Namespace: id.GetNamespace(),
		},
	}
}

func (o ObjectId) GetType() string {
	return o.ObjType
}

////////////////////////////////////////////////////////////////////////////////

type ElementId struct {
	ObjectId `json:",inline"`
	Phase    mmids.Phase `json:",inline"`
}

var _ mmids.ElementIdInfo = ElementId{}

func NewElementIdFor(id mmids.ElementIdInfo) ElementId {
	return ElementId{
		ObjectId: NewObjectIdFor(id),
		Phase:    id.GetPhase(),
	}
}

func (e ElementId) GetPhase() mmids.Phase {
	return e.Phase
}
