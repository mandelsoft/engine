package wrapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
)

func Modify[W wrapper.Object[S], S database.Object, R any](ob objectbase.Objectbase, obj W, mod func(S) (R, bool)) (R, error) {
	db := objectbase.GetDatabase[S](ob)
	o := obj.GetBase()
	r, err := database.Modify(db, &o, mod)
	obj.SetBase(o)
	return r, err
}
