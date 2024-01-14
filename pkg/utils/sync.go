package utils

import (
	"context"
)

type Sync interface {
	Wait(ctx context.Context) bool
}

type SyncTrigger interface {
	Done()
}

type syncer struct {
	state chan struct{}
}

func NewSyncPoint() (Sync, SyncTrigger) {
	s := &syncer{
		state: make(chan struct{}),
	}
	return s, s
}
func (s *syncer) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-s.state:
		return true
	}
}

func (s *syncer) Done() {
	close(s.state)
}
