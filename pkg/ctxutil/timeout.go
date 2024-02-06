package ctxutil

import (
	"context"
	"time"
)

func TimeoutContext(ctx context.Context, duration time.Duration) context.Context {
	return cancelContext(context.WithTimeout(ctx, duration))
}

func DeadlineContext(ctx context.Context, deadline time.Time) context.Context {
	return cancelContext(context.WithDeadline(ctx, deadline))
}
