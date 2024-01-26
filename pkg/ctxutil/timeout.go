package ctxutil

import (
	"context"
	"time"
)

func WatchdogContext(ctx context.Context, duration time.Duration) context.Context {
	cancel := CancelContext(ctx)
	timer := time.NewTimer(duration)

	go func() {
		select {
		case <-ctx.Done():
			Cancel(cancel)
		case <-timer.C:
			Cancel(cancel)
		}
	}()

	return cancel
}
