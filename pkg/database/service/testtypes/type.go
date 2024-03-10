package testtypes

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

type Object interface {
	db.Object

	GetData() string
}

var Scheme = db.NewScheme[Object]()
var scheme = Scheme.(database.TypeScheme[Object]) // Goland

type Status struct {
	Status string `json:"status,omitempty"`
}

func (a *Status) GetStatusValue() string {
	if a == nil {
		return ""
	}
	return a.Status
}

const TYPE_A = "A"

type A struct {
	db.ObjectMeta

	Spec   SpecA   `json:"spec"`
	Status *Status `json:"status,omitempty"`
}

type SpecA struct {
	A string `json:"a,omitempty"`
}

var _ Object = (*A)(nil)

func NewA(ns, name string, s string) *A {
	return &A{
		ObjectMeta: db.NewObjectMeta(TYPE_A, ns, name),
		Spec: SpecA{
			A: s,
		},
	}
}

func (a *A) GetStatusValue() string {
	return a.Status.GetStatusValue()
}

func (a *A) GetData() string {
	return a.Spec.A
}

const TYPE_B = "B"

type B struct {
	db.ObjectMeta

	Spec   SpecB   `json:"spec"`
	Status *Status `json:"status,omitempty"`
}

type SpecB struct {
	B string `json:"b,omitempty"`
}

var _ Object = (*B)(nil)

func NewB(ns, name string, s string) *B {
	return &B{
		ObjectMeta: db.NewObjectMeta(TYPE_B, ns, name),
		Spec: SpecB{
			B: s,
		},
	}
}

func (a *B) GetStatusValue() string {
	return a.Status.GetStatusValue()
}

func (b *B) GetData() string {
	return b.Spec.B
}

func init() {
	database.MustRegisterType[A](scheme) // Goland
	database.MustRegisterType[B](scheme)
}
