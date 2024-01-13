package filesystem_test

import (
	"github.com/mandelsoft/engine/pkg/database"
)

var Scheme = database.NewScheme()

const TYPE_A = "A"

type A struct {
	database.TypedObject

	A string `json:"a,omitempty"`
}

func NewA(ns, name string, s string) *A {
	return &A{
		TypedObject: database.NewTypedObject(TYPE_A, ns, name),
		A:           s,
	}
}

const TYPE_B = "B"

type B struct {
	database.TypedObject

	B string `json:"b,omitempty"`
}

func NewB(ns, name string, s string) *B {
	return &B{
		TypedObject: database.NewTypedObject(TYPE_B, ns, name),
		B:           s,
	}
}

func init() {
	database.MustRegisterType[A](Scheme)
	database.MustRegisterType[B](Scheme)
}
