package database

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/utils"
)

// Modify modifies an object with modifier mod taking race conditions
// int account. The finally modified object is returned.
// If an errors is returned the object state might be corrupted.
// O must be a suv type of DBO (Go does not support the upper bound operator.
func Modify[O Object, R any, DBO Object](db Database[DBO], obj *O, mod func(O) (R, bool)) (R, error) {
	o := *obj
	for {
		r, modified := mod(o)
		if modified {
			err := db.SetObject(utils.Cast[DBO](o))
			if err != nil {
				if errors.Is(err, ErrModified) {
					_o, err := db.GetObject(o)
					if err == nil {
						var ok bool
						o, ok = utils.TryCast[O](_o)
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
