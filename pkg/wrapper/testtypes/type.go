package testtypes

import (
	"github.com/mandelsoft/engine/pkg/database"
	dbtypes "github.com/mandelsoft/engine/pkg/impl/database/filesystem/testtypes"
	me "github.com/mandelsoft/engine/pkg/wrapper"
	"github.com/mandelsoft/engine/pkg/wrapper/support"
)

var Scheme = me.NewTypeScheme[Object](dbtypes.Scheme)
var scheme = Scheme.(database.TypeScheme[Object])

type Object interface {
	me.Object[dbtypes.Object]

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
