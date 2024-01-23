package testtypes

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/database/wrapper/support"
	dbtypes "github.com/mandelsoft/engine/pkg/impl/database/filesystem/testtypes"
)

var Scheme = wrapper.NewTypeScheme[Object](dbtypes.Scheme)
var scheme = Scheme.(database.TypeScheme[Object])

type Object interface {
	wrapper.Object[dbtypes.Object]

	GetData() string
	SetData(string)
}

const TYPE_A = "A"

type A struct {
	support.Wrapper[dbtypes.Object]
}

var _ Object = (*A)(nil)

func NewA(ns, name string, s string) *A {
	return &A{
		Wrapper: support.NewDBWrapper[dbtypes.Object](dbtypes.NewA(ns, name, s)),
	}
}

func (a *A) GetData() string {
	return a.GetBase().GetData()
}

func (a *A) SetData(s string) {
	a.GetBase().(*dbtypes.A).A = s
}

const TYPE_B = "B"

type B struct {
	support.Wrapper[dbtypes.Object]
}

func NewB(ns, name string, s string) *B {
	return &B{
		Wrapper: support.NewDBWrapper[dbtypes.Object](dbtypes.NewB(ns, name, s)),
	}
}

func (b *B) GetData() string {
	return b.GetBase().GetData()
}

func (b *B) SetData(s string) {
	b.GetBase().(*dbtypes.B).B = s
}

func init() {
	database.MustRegisterType[A](Scheme)
	database.MustRegisterType[B](Scheme)
}
