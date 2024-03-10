package db

import (
	"github.com/mandelsoft/engine/pkg/database"
)

const APIVERSION = "engine/v1"

type ObjectMetaAccessor interface {
	database.Object
	database.GenerationAccess
	database.Finalizable
}

type Object interface {
	ObjectMetaAccessor
	database.StatusSource
}

type ObjectMeta struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	MetaData   MetaData `json:"metadata"`
}

var _ ObjectMetaAccessor = (*ObjectMeta)(nil)

func (o *ObjectMeta) GetFinalizers() []string {
	return o.MetaData.GetFinalizers()
}

func (o *ObjectMeta) SetFinalizers(f []string) {
	o.MetaData.SetFinalizers(f)
}

func (o *ObjectMeta) HasFinalizer(f string) bool {
	return o.MetaData.HasFinalizer(f)
}

func (o *ObjectMeta) AddFinalizer(f string) bool {
	return o.MetaData.AddFinalizer(f)
}

func (o *ObjectMeta) RemoveFinalizer(f string) bool {
	return o.MetaData.RemoveFinalizer(f)
}

func (o *ObjectMeta) RequestDeletion() {
	o.MetaData.RequestDeletion()
}

func (o *ObjectMeta) IsDeleting() bool {
	return o.MetaData.IsDeleting()
}

func (o *ObjectMeta) SetName(s string) {
	o.MetaData.SetName(s)
}

func (o *ObjectMeta) SetNamespace(s string) {
	o.MetaData.SetNamespace(s)
}

func (o *ObjectMeta) SetType(s string) {
	o.Kind = s
	o.APIVersion = APIVERSION
}

func (o *ObjectMeta) GetNamespace() string {
	return o.MetaData.GetNamespace()
}

func (o *ObjectMeta) GetName() string {
	return o.MetaData.GetName()
}

func (o *ObjectMeta) GetType() string {
	return o.Kind
}

func (o *ObjectMeta) GetGeneration() int64 {
	return o.MetaData.GetGeneration()
}

func (o *ObjectMeta) SetGeneration(i int64) {
	o.MetaData.SetGeneration(i)
}

type MetaData struct {
	database.Named         `json:",inline"`
	database.Generation    `json:",inline"`
	database.FinalizedMeta `json:",inline"`
}

func NewObjectMeta(ty string, ns string, name string) ObjectMeta {
	return ObjectMeta{
		APIVersion: APIVERSION,
		Kind:       ty,
		MetaData: MetaData{
			Named: database.Named{Name: name, Namespace: ns},
		},
	}
}
