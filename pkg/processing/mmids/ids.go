package mmids

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mandelsoft/engine/pkg/database"
)

type RunId string

func (r RunId) String() string {
	return string(r)
}

type Phase string

func (p Phase) String() string {
	return string(p)
}

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

type ElementIdInfo interface {
	database.ObjectId
	GetPhase() Phase
}

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

func NewElementIdForTypePhase(typ string, n NameSource, ph Phase) ElementId {
	return NewElementId(typ, n.GetNamespace(), n.GetName(), ph)
}

func NewElementIdForType(typ TypeId, ns, name string) ElementId {
	return NewElementId(typ.GetType(), ns, name, typ.GetPhase())
}

func NewElementIdForObject(typ TypeId, o database.ObjectId) ElementId {
	return NewElementId(typ.GetType(), o.GetNamespace(), o.GetName(), typ.GetPhase())
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

func CompareElementId(a, b ElementId) int {
	d := strings.Compare(a.GetType(), b.GetType())
	if d == 0 {
		d = strings.Compare(a.GetNamespace(), b.GetNamespace())
	}
	if d == 0 {
		d = strings.Compare(a.GetName(), b.GetName())
	}
	return d
}
