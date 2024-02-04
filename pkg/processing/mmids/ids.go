package mmids

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/mandelsoft/engine/pkg/database"
)

type RunId string
type Phase string

func NewRunId() RunId {
	return RunId(uuid.NewString())
}

type NameSource interface {
	GetName() string
	GetNamespace() string
}

type ObjectIdSource interface {
	NameSource
	GetType() string
}

var _ ObjectIdSource = (database.ObjectId)(nil)

type ObjectId struct {
	objtype   string
	namespace string
	name      string
}

func NewObjectId(typ, namespace, name string) ObjectId {
	return ObjectId{
		objtype:   typ,
		namespace: namespace,
		name:      name,
	}
}

func NewObjectIdFor(o database.ObjectId) ObjectId {
	return ObjectId{
		objtype:   o.GetType(),
		namespace: o.GetNamespace(),
		name:      o.GetName(),
	}
}

func (o ObjectId) GetType() string {
	return o.objtype
}

func (o ObjectId) GetNamespace() string {
	return o.namespace
}

func (o ObjectId) GetName() string {
	return o.name
}

func (o ObjectId) String() string {
	return fmt.Sprintf("%s/%s/%s", o.objtype, o.namespace, o.name)
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
	return TypeId{objtype: e.objtype, phase: e.phase}
}

func (e ElementId) String() string {
	return fmt.Sprintf("%s:%s", e._objectId, e.phase)
}
