package utils

import (
	"sync/atomic"
)

type AtomicValue[T any] struct {
	atomic.Value
}

func (v *AtomicValue[T]) Load() T {
	return v.Value.Load().(T)
}

func (v *AtomicValue[T]) Store(new T) {
	v.Value.Store(new)
}

func (v *AtomicValue[T]) Swap(new T) T {
	return v.Value.Swap(new).(T)
}

func (v *AtomicValue[T]) CompareAndSwap(old, new T) bool {
	return v.Value.CompareAndSwap(old, new)
}
