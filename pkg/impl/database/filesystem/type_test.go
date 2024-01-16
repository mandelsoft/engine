package filesystem_test

import (
	"github.com/mandelsoft/engine/pkg/database"
)

var Scheme = database.NewScheme()

const TYPE_A = "A"

type A struct {
	database.GenerationObjectMeta

	A string `json:"a,omitempty"`
}

var _ database.GenerationAccess = (*A)(nil)

func NewA(ns, name string, s string) *A {
	return &A{
		GenerationObjectMeta: database.NewGenerationObjectMeta(TYPE_A, ns, name),
		A:                    s,
	}
}

const TYPE_B = "B"

type B struct {
	database.GenerationObjectMeta

	B string `json:"b,omitempty"`
}

func NewB(ns, name string, s string) *B {
	return &B{
		GenerationObjectMeta: database.NewGenerationObjectMeta(TYPE_B, ns, name),
		B:                    s,
	}
}

func init() {
	database.MustRegisterType[A](Scheme)
	database.MustRegisterType[B](Scheme)
}
