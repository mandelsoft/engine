package database

import (
	"fmt"
	"slices"

	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
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

func NewScheme[O Object](e runtime.TypeExtractor) Scheme[O] {
	return runtime.NewYAMLScheme[O](e)
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

// Finalizable is an optional interface of
// objects featuring a deletion state and finalizers.
type Finalizable interface {
	GetFinalizers() []string
	SetFinalizers(f []string)
	HasFinalizer(f string) bool
	AddFinalizer(f string) bool
	RemoveFinalizer(f string) bool

	RequestDeletion()
	IsDeleting() bool
	GetDeletionInfo() DeletionInfo
	PreserveDeletion(DeletionInfo)
}

var ErrModified = fmt.Errorf("object modified")
var ErrNotExist = fmt.Errorf("object not found")

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

type DeletionInfo any

type FinalizedMeta struct {
	Finalizers   []string         `json:"finalizers,omitempty"`
	DeletionTime *utils.Timestamp `json:"deletionTime,omitempty"`
}

var _ Finalizable = (*FinalizedMeta)(nil)

func (g *FinalizedMeta) IsDeleting() bool {
	return g.DeletionTime != nil
}

func (g *FinalizedMeta) GetDeletionInfo() DeletionInfo {
	return g.DeletionTime
}

func (g *FinalizedMeta) PreserveDeletion(info DeletionInfo) {
	if g.DeletionTime == nil && info != nil {
		g.DeletionTime = info.(*utils.Timestamp)
	}
}

func (g *FinalizedMeta) RequestDeletion() {
	if g.IsDeleting() {
		return
	}
	Log.Debug("requesting deletion")
	g.DeletionTime = utils.NewTimestampP()
}

func (g *FinalizedMeta) GetFinalizers() []string {
	return slices.Clone(g.Finalizers)
}

func (g *FinalizedMeta) SetFinalizers(f []string) {
	g.Finalizers = slices.Clone(f)
}

func (g *FinalizedMeta) AddFinalizer(f string) bool {
	if !slices.Contains(g.Finalizers, f) {
		g.Finalizers = append(g.Finalizers, f)
		return true
	}
	return false
}

func (g *FinalizedMeta) HasFinalizer(f string) bool {
	if slices.Contains(g.Finalizers, f) {
		return true
	}
	return false
}

func (g *FinalizedMeta) RemoveFinalizer(f string) bool {
	i := slices.Index(g.Finalizers, f)
	if i < 0 {
		return false
	}
	g.Finalizers = append(g.Finalizers[:i], g.Finalizers[i+1:]...)
	return true
}

////////////////////////////////////////////////////////////////////////////////

type StatusSource interface {
	GetStatusValue() string
}

////////////////////////////////////////////////////////////////////////////////

type Named struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (n Named) GetName() string {
	return n.Name
}

func (n Named) GetNamespace() string {
	return n.Namespace
}

func (n *Named) SetName(name string) {
	n.Name = name
}

func (n *Named) SetNamespace(name string) {
	n.Namespace = name
}

func (n Named) String() string {
	return fmt.Sprintf("%s/%s", n.Namespace, n.Name)
}

////////////////////////////////////////////////////////////////////////////////

type ObjectId interface {
	GetNamespace() string
	GetName() string

	runtime.TypeAccessor
}

type LocalObjectId interface {
	runtime.TypeAccessor
	GetName() string
}

type LocalObjectRef struct {
	runtime.ObjectMeta `json:",inline"`
	Name               string `json:"name"`
}

var _ LocalObjectId = (*LocalObjectRef)(nil)

func NewLocalObjectRef(typ, name string) LocalObjectRef {
	return LocalObjectRef{runtime.ObjectMeta{typ}, name}
}

func NewLocalObjectRefFor(id ObjectId) LocalObjectRef {
	return LocalObjectRef{
		ObjectMeta: runtime.ObjectMeta{id.GetType()},
		Name:       id.GetName(),
	}
}

func (r LocalObjectRef) GetName() string {
	return r.Name
}

func (r LocalObjectRef) String() string {
	return fmt.Sprintf("%s:%s", r.Type, r.Name)
}

func (r LocalObjectRef) In(ns string) ObjectId {
	return NewObjectId(r.Type, ns, r.Name)
}

type ObjectRef struct {
	runtime.ObjectMeta `json:",inline"`
	Named              `json:",inline"`
}

var _ ObjectId = (*ObjectRef)(nil)

func NewObjectRef(typ, ns, name string) ObjectRef {
	return ObjectRef{runtime.ObjectMeta{typ}, Named{Name: name, Namespace: ns}}
}

func NewObjectRefFor(id ObjectId) ObjectRef {
	return ObjectRef{
		ObjectMeta: runtime.ObjectMeta{id.GetType()},
		Named:      Named{id.GetNamespace(), id.GetName()},
	}
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

func (o objectid) GetName() string {
	return o.name
}

func (o objectid) GetNamespace() string {
	return o.namespace
}

func (o objectid) GetType() string {
	return o.kind
}

func (o objectid) String() string {
	return fmt.Sprintf("%s/%s/%s", o.kind, o.namespace, o.name)
}

func NewObjectId(typ, ns, name string) ObjectId {
	return objectid{typ, ns, name}
}

func NewObjectIdFor(id ObjectId) ObjectId {
	if eff, ok := id.(objectid); ok {
		return eff
	}
	return objectid{
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
