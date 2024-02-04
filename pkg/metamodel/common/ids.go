package common

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
)

type ObjectId struct {
	data database.ObjectRef
}

func NewObjectId(typ, namespace, name string) ObjectId {
	return ObjectId{
		data: database.NewObjectRef(typ, namespace, name),
	}
}

func NewObjectIdFor(o Object) ObjectId {
	return ObjectId{
		data: database.NewObjectRefFor(o),
	}
}

func (o ObjectId) GetType() string {
	return o.data.Type
}

func (o ObjectId) GetNamespace() string {
	return o.data.Namespace
}

func (o ObjectId) GetName() string {
	return o.data.Name
}

func (o ObjectId) String() string {
	return fmt.Sprintf("%s/%s/%s", o.data.Type, o.data.Namespace, o.data.Name)
}

type _objectId = ObjectId

////////////////////////////////////////////////////////////////////////////////

type TypeId struct {
	objtype string
	phase   Phase
}

func NewTypeId(typ string, phase Phase) TypeId {
	return TypeId{
		objtype: typ,
		phase:   phase,
	}
}

func (o TypeId) GetType() string {
	return o.objtype
}

func (o TypeId) GetPhase() Phase {
	return o.phase
}

func (o TypeId) String() string {
	return fmt.Sprintf("%s:%s", o.objtype, o.phase)
}

////////////////////////////////////////////////////////////////////////////////

type ElementId struct {
	_objectId
	phase Phase
}

func NewElementId(typ, namespace, name string, phase Phase) ElementId {
	return ElementId{
		_objectId: NewObjectId(typ, namespace, name),
		phase:     phase,
	}
}

func NewElementIdForPhase(oid database.ObjectId, ph Phase) ElementId {
	return NewElementId(oid.GetType(), oid.GetNamespace(), oid.GetName(), ph)
}

func NewElementIdForType(typ TypeId, ns, name string) ElementId {
	return NewElementId(typ.GetType(), ns, name, typ.GetPhase())
}

func (e ElementId) GetPhase() Phase {
	return e.phase
}

func (e ElementId) ObjectId() ObjectId {
	return e._objectId
}

func (e ElementId) TypeId() TypeId {
	return TypeId{objtype: e.data.Type, phase: e.phase}
}

func (e ElementId) String() string {
	return fmt.Sprintf("%s:%s", e._objectId, e.phase)
}
