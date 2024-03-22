package database

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/goutils/generics"
)

// Modify modifies an object with modifier mod taking race conditions
// int account. The finally modified object is returned.
// If an errors is returned the object state might be corrupted.
// O must be a sub type of DBO (Go does not support the upper bound operator.
func Modify[O Object, R any, DBO Object](db Database[DBO], obj *O, mod func(O) (R, bool)) (R, error) {
	o := *obj
	for {
		r, modified := mod(o)
		if modified {
			err := db.SetObject(generics.Cast[DBO](o))
			if err != nil {
				if errors.Is(err, ErrModified) {
					_o, err := db.GetObject(o)
					if err == nil {
						var ok bool
						o, ok = generics.TryCast[O](_o)
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

func CreateOrModify[O Object, DBO Object](db Database[DBO], obj *O, mod func(O) bool) (bool, error) {
	for {
		m := false
		_o, err := db.GetObject(*obj)
		if err != nil {
			if !errors.Is(err, ErrNotExist) {
				return false, err
			}
			_o = generics.Cast[DBO](*obj)
			err = nil
			m = true
		}
		o, ok := generics.TryCast[O](_o)
		if !ok {
			return false, fmt.Errorf("non-matching Go type %T for %q", _o, _o.GetType())
		}
		ok = mod(o) || m
		if ok {
			err = db.SetObject(_o)
			if errors.Is(err, ErrModified) {
				continue
			}
		}
		return ok, err
	}
}

func ModifyExisting[O Object, DBO Object](db Database[DBO], obj *O, mod func(O) bool) (bool, error) {
	for {
		_o, err := db.GetObject(*obj)
		if err != nil {
			if !errors.Is(err, ErrNotExist) {
				return false, err
			}
			return false, nil
		}
		o, ok := generics.TryCast[O](_o)
		if !ok {
			return false, fmt.Errorf("non-matching Go type %T for %q", _o, _o.GetType())
		}
		ok = mod(o)
		if ok {
			err = db.SetObject(_o)
			if errors.Is(err, ErrModified) {
				continue
			}
		}
		return ok, err
	}
}

func IsModified[O Object, DBO Object](db Database[DBO], obj *O, mod func(O) bool) (bool, error) {
	_o, err := db.GetObject(*obj)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return false, err
		}
		return true, nil
	}
	o, ok := generics.TryCast[O](_o)
	if !ok {
		return false, fmt.Errorf("non-matching Go type %T for %q", _o, _o.GetType())
	}
	ok = mod(o)
	return ok, err
}

// DirectModify is like Modify, but does not do retries.
// The first run must work on a up-to-date object.
// It returns true, if this update succeeds.
// If the object is outdated, it returns false.
func DirectModify[O Object, DBO Object](db Database[DBO], obj *O, mod func(O) bool) (bool, error) {
	retry := false
	return Modify(db, obj, func(o O) (bool, bool) {
		if retry {
			return false, false
		}
		retry = true
		return true, mod(o)
	})
}
