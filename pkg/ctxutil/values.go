/*
 * SPDX-FileCopyrightText: 2020 SAP SE or an SAP affiliate company and Gardener contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package ctxutil

import (
	"context"
)

type ValueKey[T any] interface {
	Name() string
	WithValue(ctx context.Context, value T) context.Context
	Get(ctx context.Context) T
}

type valueKey[T any] struct {
	key Key
}

func NewValueKey[T any](name string) ValueKey[T] {
	return &valueKey[T]{
		key: SimpleKey(name),
	}
}

func (this *valueKey[T]) Name() string {
	return this.key.String()
}

func (this *valueKey[T]) WithValue(ctx context.Context, value T) context.Context {
	return context.WithValue(ctx, this.key, value)
}

func (this *valueKey[T]) Get(ctx context.Context) T {
	return ctx.Value(this.key).(T)
}
