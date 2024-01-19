package utils

func TryCast[T, O any](o O) (T, bool) {
	var i any = o
	t, ok := i.(T)
	return t, ok
}
