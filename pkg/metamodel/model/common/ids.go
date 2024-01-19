package common

import (
	"fmt"
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

type ElementId struct {
	_objectId
	phase Phase
}

func NewElementId(typ, namespace, name string, phase Phase) ElementId {
	return ElementId{
		_objectId: ObjectId{
			objtype:   typ,
			objname:   name,
			namespace: namespace,
		},
		phase: phase,
	}
}

func (e ElementId) Phase() Phase {
	return e.phase
}

func (e ElementId) String() string {
	return fmt.Sprintf("%s:%s", e._objectId, e.phase)
}