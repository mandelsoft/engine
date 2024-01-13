package database

import (
	"fmt"
	"reflect"

	"github.com/mandelsoft/engine/pkg/runtime"
)

type RunId string

type Scheme = runtime.Scheme[Object]

func NewScheme() Scheme {
	return runtime.NewScheme[Object]()
}

type Object interface {
	ObjectId
}

type ObjectId interface {
	GetNamespace() string
	GetName() string

	runtime.Object
}

type TypedObject = objectid

func NewTypedObject(typ, ns, name string) TypedObject {
	return objectid{typ, ns, name}
}

type objectid struct {
	Type      string
	Namespace string
	Name      string
}

func (o *objectid) GetName() string {
	return o.Name
}

func (o *objectid) GetNamespace() string {
	return o.Namespace
}

func (o *objectid) GetType() string {
	return o.Type
}

func NewObjectId(typ, ns, name string) ObjectId {
	return &objectid{typ, ns, name}
}

func NewObjectIdFor(id ObjectId) ObjectId {
	return &objectid{
		Type:      id.GetType(),
		Namespace: id.GetNamespace(),
		Name:      id.GetName(),
	}
}

func EqualObjectId(a, b ObjectId) bool {
	return reflect.DeepEqual(NewObjectIdFor(a), NewObjectIdFor(b))
}

func StringId(a ObjectId) string {
	return fmt.Sprintf("%s/%s/%s", a.GetType(), a.GetNamespace(), a.GetName())
}
