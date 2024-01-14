package database

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/runtime"
)

type Scheme = runtime.Scheme[Object]
type Encoding = runtime.Encoding[Object]

func NewScheme() Scheme {
	return runtime.NewYAMLScheme[Object]()
}

type ObjectMetaAccessor interface {
	ObjectId
}

type Object interface {
	ObjectMetaAccessor
}

type ObjectId interface {
	GetNamespace() string
	GetName() string

	runtime.Object
}

type ObjectRef struct {
	runtime.ObjectMeta `json:",inline"`
	Namespace          string `json:"namespace"`
	Name               string `json:"name"`
}

var _ ObjectId = (*ObjectRef)(nil)

func NewObjectRef(typ, ns, name string) ObjectRef {
	return ObjectRef{runtime.ObjectMeta{typ}, ns, name}
}

func NewObjectRefFor(id ObjectId) ObjectRef {
	return ObjectRef{
		ObjectMeta: runtime.ObjectMeta{id.GetType()},
		Namespace:  id.GetNamespace(),
		Name:       id.GetName(),
	}
}

func (o *ObjectRef) GetName() string {
	return o.Name
}

func (o *ObjectRef) GetNamespace() string {
	return o.Namespace
}

type ObjectMeta struct {
	ObjectRef `json:",inline"`
}

var _ ObjectMetaAccessor = (*ObjectMeta)(nil)

func NewObjectMeta(typ, ns, name string) ObjectMeta {
	return ObjectMeta{NewObjectRef(typ, ns, name)}
}

////////////////////////////////////////////////////////////////////////////////

type objectid struct {
	kind      string
	namespace string
	name      string
}

func (o *objectid) GetName() string {
	return o.name
}

func (o *objectid) GetNamespace() string {
	return o.namespace
}

func (o *objectid) GetType() string {
	return o.kind
}

func NewObjectId(typ, ns, name string) ObjectId {
	return &objectid{typ, ns, name}
}

func NewObjectIdFor(id ObjectId) ObjectId {
	return &objectid{
		kind:      id.GetType(),
		namespace: id.GetNamespace(),
		name:      id.GetName(),
	}
}

func EqualObjectId(a, b ObjectId) bool {
	return a.GetType() == b.GetType() &&
		a.GetNamespace() == b.GetNamespace() &&
		a.GetName() == b.GetName()
}

func StringId(a ObjectId) string {
	return fmt.Sprintf("%s/%s/%s", a.GetType(), a.GetNamespace(), a.GetName())
}
