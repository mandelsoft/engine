package service

import (
	"context"
	"errors"
	"sync"

	"github.com/mandelsoft/engine/pkg/future"
)

type Syncher interface {
	SetError(err error)
	Wait() error
}

func Sync(wg *sync.WaitGroup) Syncher {
	return &syncher{
		wait: wg,
	}
}

type syncher struct {
	lock sync.Mutex
	wait *sync.WaitGroup
	err  []error
}

func (s *syncher) SetError(err error) {
	if err != nil {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.err = append(s.err, err)
	}
}

func (s *syncher) Wait() error {
	s.wait.Wait()
	s.lock.Lock()
	defer s.lock.Unlock()
	return errors.Join(s.err...)
}

type Trigger interface {
	Syncher
	Trigger()
}

func SyncTrigger() Trigger {
	return &trigger{
		trigger: future.NewFuture(true),
	}
}

type trigger struct {
	err     error
	trigger future.Trigger
}

var _ Trigger = (*trigger)(nil)

func (t *trigger) Trigger() {
	t.trigger.Trigger()
}

func (t *trigger) SetError(err error) {
	t.err = err
}

func (t *trigger) Wait() error {
	t.trigger.Wait(context.Background())
	return t.err
}
