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

func CastPointer[T any, E any, P PointerType[E]](e P) T {
	var _nil T
	if e == nil {
		return _nil
	}
	var i any = e
	return i.(T)
}

func Pointer[T any](t T) *T {
	return &t
}

func TypeOf[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func ConvertSlice[D, S any](in []S) []D {
	if TypeOf[D]() == TypeOf[S]() {
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

func AssertType[C any]() C {
	var _nil C
	return _nil
}

// CastSlice casts a slice by casting the element types.
// The slice is copied.
// T MUST be a super type of E.
func CastSlice[T any, S ~[]E, E any](s S) []T {
	// Preserve nil in case it matters.
	if s == nil {
		return nil
	}
	t := make([]T, len(s))
	for i, e := range s {
		t[i] = Cast[T](e)
	}
	return t
}

type PointerType[P any] interface {
	*P
}

func CastPointerSlice[T any, S ~[]P, E any, P PointerType[E]](s S) []T {
	var _nil T

	// Preserve nil in case it matters.
	if s == nil {
		return nil
	}
	t := make([]T, len(s))
	for i, e := range s {
		if e == nil {
			t[i] = _nil
		} else {
			t[i] = Cast[T](e)
		}
	}
	return t
}
