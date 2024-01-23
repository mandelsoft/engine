package testtypes

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Object interface {
	database.Object
	database.GenerationAccess

	GetData() string
}

var Scheme = database.NewScheme[Object]()
var scheme = Scheme.(database.TypeScheme[Object]) // Goland

const TYPE_A = "A"

type A struct {
	database.GenerationObjectMeta

	A string `json:"a,omitempty"`
}

var _ Object = (*A)(nil)

func NewA(ns, name string, s string) *A {
	return &A{
		GenerationObjectMeta: database.NewGenerationObjectMeta(TYPE_A, ns, name),
		A:                    s,
	}
}

func (a *A) GetData() string {
	return a.A
}

const TYPE_B = "B"

type B struct {
	database.GenerationObjectMeta

	B string `json:"b,omitempty"`
}

var _ Object = (*B)(nil)

func NewB(ns, name string, s string) *B {
	return &B{
		GenerationObjectMeta: database.NewGenerationObjectMeta(TYPE_B, ns, name),
		B:                    s,
	}
}

func (b *B) GetData() string {
	return b.B
}

func init() {
	database.MustRegisterType[A](scheme) // Goland
	database.MustRegisterType[B](scheme)
}
