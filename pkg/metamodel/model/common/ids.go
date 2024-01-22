package common

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
)

type ObjectId struct {
	objtype   string
	objname   string
	namespace string
}

func NewObjectId(typ, namespace, name string) ObjectId {
	return ObjectId{
		objtype:   typ,
		objname:   name,
		namespace: namespace,
	}
}

func NewObjectIdFor(o Object) ObjectId {
	return ObjectId{
		objtype:   o.GetType(),
		objname:   o.GetName(),
		namespace: o.GetNamespace(),
	}
}

func (o ObjectId) Type() string {
	return o.objtype
}

func (o ObjectId) Namespace() string {
	return o.namespace
}

func (o ObjectId) Name() string {
	return o.objname
}

func (o ObjectId) String() string {
	return fmt.Sprintf("%s/%s/%s", o.objtype, o.namespace, o.objname)
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

func (o TypeId) Type() string {
	return o.objtype
}

func (o TypeId) Phase() Phase {
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
		_objectId: ObjectId{
			objtype:   typ,
			namespace: namespace,
			objname:   name,
		},
		phase: phase,
	}
}

func NewElementIdForPhase(oid database.ObjectId, ph Phase) ElementId {
	return NewElementId(oid.GetType(), oid.GetNamespace(), oid.GetName(), ph)
}

func (e ElementId) Phase() Phase {
	return e.phase
}

func (e ElementId) ObjectId() ObjectId {
	return e._objectId
}

func (e ElementId) TypeId() TypeId {
	return TypeId{objtype: e.objtype, phase: e.phase}
}

func (e ElementId) String() string {
	return fmt.Sprintf("%s:%s", e._objectId, e.phase)
}
