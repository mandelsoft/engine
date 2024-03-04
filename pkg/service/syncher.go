package service

import (
	"errors"
	"sync"
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
		trigger: make(chan struct{}),
	}
}

type trigger struct {
	err     error
	trigger chan struct{}
}

var _ Trigger = (*trigger)(nil)

func (t *trigger) Trigger() {
	select {
	case <-t.trigger:
	default:
		close(t.trigger)
	}
}

func (t *trigger) SetError(err error) {
	t.err = err
}

func (t *trigger) Wait() error {
	<-t.trigger
	return t.err
}
