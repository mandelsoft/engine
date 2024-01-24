package utils

import (
	"reflect"
)

func Optional[T any](args ...T) T {
	var _nil T
	for _, e := range args {
		if !reflect.DeepEqual(e, _nil) {
			return e
		}
	}
	return _nil
}

func OptionalDefaulted[T any](def T, args ...T) T {
	var _nil T
	for _, e := range args {
		if !reflect.DeepEqual(e, _nil) {
			return e
		}
	}
	return def
}
