package utils

import (
	"reflect"
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
