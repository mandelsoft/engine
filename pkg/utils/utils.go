package utils

import (
	"cmp"
	"reflect"
	"slices"
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

func MapKeys[K comparable, V any](m map[K]V) []K {
	r := []K{}

	for k := range m {
		r = append(r, k)
	}
	return r
}

func OrderedMapKeys[K cmp.Ordered, V any](m map[K]V) []K {
	r := MapKeys(m)
	slices.Sort(r)
	return r
}
