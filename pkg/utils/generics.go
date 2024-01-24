package utils

import (
	"cmp"
	"reflect"
	"slices"
	"strings"
)

func TryCast[T, O any](o O) (T, bool) {
	var i any = o
	t, ok := i.(T)
	return t, ok
}

func Cast[T, O any](o O) T {
	var i any = o
	t := i.(T)
	return t
}

func Pointer[T any](t T) *T {
	return &t
}

func TypeOf[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func ConvertSlice[D, S any](in []S) []D {
	if TypeOf[D]() == TypeOf[D]() {
		return Cast[[]D](in)
	}
	var r []D
	for _, e := range in {
		r = append(r, Cast[D](e))
	}
	return r
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

type stringable interface {
	String() string
}

func CompareStringable[T stringable](a, b T) int {
	return strings.Compare(a.String(), b.String())
}

func Join[S stringable](list []S) string {
	sep := ""
	r := ""
	for _, e := range list {
		r += sep + e.String()
		sep = ", "
	}
	return r
}
