package database

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/runtime"
)

type Scheme[O Object] interface {
	runtime.Scheme[O]
}

type TypeScheme[O Object] interface {
	runtime.TypeScheme[O]
}

type Encoding[O Object] interface {
	runtime.Encoding[O]
}

func NewScheme[O Object]() Scheme[O] {
	return runtime.NewYAMLScheme[O]()
}

type ObjectMetaAccessor interface {
	ObjectId
	runtime.Object
}

type Object interface {
	ObjectMetaAccessor
	SetName(string)
	SetNamespace(string)
}

// GenerationAccess is an optional Object interface
// for objects featuring a generation number.
// This is required for race condition detection
// in updates.
type GenerationAccess interface {
	GetGeneration() int64
	SetGeneration(int64)
}

var ErrModified = fmt.Errorf("object modified")
var ErrNotExist = fmt.Errorf("object not found")

type Generation struct {
	Generation int64 `json:"generation"`
}

func (g *Generation) GetGeneration() int64 {
	return g.Generation
}

func (g *Generation) SetGeneration(i int64) {
	g.Generation = i
}

func GetGeneration(o Object) int64 {
	if g, ok := o.(GenerationAccess); ok {
		return g.GetGeneration()
	}
	return -1
}

type StatusSource interface {
	GetStatusValue() string
}

type ObjectId interface {
	GetNamespace() string
	GetName() string

	runtime.TypeAccessor
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

func (o ObjectRef) GetName() string {
	return o.Name
}

func (o ObjectRef) GetNamespace() string {
	return o.Namespace
}

func (o *ObjectRef) SetName(name string) {
	o.Name = name
}

func (o *ObjectRef) SetNamespace(name string) {
	o.Namespace = name
}

func (o ObjectRef) String() string {
	return fmt.Sprintf("%s/%s/%s", o.Type, o.Namespace, o.Name)
}

type ObjectMeta struct {
	ObjectRef `json:",inline"`
}

type GenerationObjectMeta struct {
	ObjectMeta `json:",inline"`
	Generation
}

var _ ObjectMetaAccessor = (*ObjectMeta)(nil)

func NewObjectMeta(typ, ns, name string) ObjectMeta {
	return ObjectMeta{NewObjectRef(typ, ns, name)}
}

func NewGenerationObjectMeta(typ, ns, name string) GenerationObjectMeta {
	return GenerationObjectMeta{ObjectMeta: NewObjectMeta(typ, ns, name)}
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

func (o *objectid) String() string {
	return fmt.Sprintf("%s/%s/%s", o.kind, o.namespace, o.name)
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
