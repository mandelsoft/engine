package objectbase

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
)

func Modify[O Object, R any](ob Objectbase, obj *O, mod func(O) (R, bool)) (R, error) {
	o := *obj
	for {
		r, modified := mod(o)
		if modified {
			err := ob.SetObject(o)
			if err != nil {
				if errors.Is(err, database.ErrModified) {
					_o, err := ob.GetObject(o)
					if err == nil {
						var ok bool
						o, ok = _o.(O)
						if !ok {
							return r, fmt.Errorf("non-matching Go type %T for %q", _o, _o.GetType())
						}
						continue
					}
				}
				return r, err
			}
		}
		*obj = o
		return r, nil
	}
}

////////////////////////////////////////////////////////////////////////////////

func SetObjectName(ns, n string) Initializer {
	return database.SetObjectName[Object](ns, n)
}