package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Service interface {
	Start(ctx context.Context) (ready Syncher, done Syncher, err error)
	Wait() error
}

type Services interface {
	Add(s Service) error
	Start(st ...Service) error
	Wait() error
}

type services struct {
	lock     sync.Mutex
	ctx      context.Context
	services map[Service]Syncher
	started  bool
	wg       *sync.WaitGroup
	errs     []error
}

func New(ctx context.Context) Services {
	return &services{
		ctx:      ctxutil.CancelContext(ctx),
		services: map[Service]Syncher{},
		wg:       &sync.WaitGroup{},
	}
}

func (t *services) Add(s Service) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.started {
		ready, err := t.start(s)
		if err != nil {
			return err
		}
		if ready != nil {
			err := ready.Wait()
			if err != nil {
				ctxutil.Cancel(t.ctx)
				return err
			}
		}
	} else {
		t.services[s] = nil
	}
	return nil
}

func (t *services) Start(st ...Service) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(st) == 0 {
		if !t.started {
			t.started = true
			return t.startServices(utils.MapKeys(t.services)...)
		}
	} else {
		return t.startServices(st...)
	}
	return nil
}

func (t *services) startServices(list ...Service) error {
	var ready []Syncher
	for _, s := range list {
		sy := t.services[s]
		if sy == nil {
			r, err := t.start(s)
			if err != nil {
				return err
			}
			if r != nil {
				ready = append(ready, r)
			}
		}
	}

	for _, r := range ready {
		err := r.Wait()
		if err != nil {
			ctxutil.Cancel(t.ctx)
			return err
		}
	}
	return nil
}

func (t *services) start(s Service) (Syncher, error) {
	ready, done, err := s.Start(t.ctx)
	if err != nil || done == nil {
		ctxutil.Cancel(t.ctx)
		if err == nil && done == nil {
			err = fmt.Errorf("service %T does not return a done syncher", s)
		} else {
			err = fmt.Errorf("service %T: %w", s, err)
		}
		return nil, err
	}
	t.services[s] = done
	t.wg.Add(1)
	go func() {
		err := done.Wait()
		if err != nil {
			t.lock.Lock()
			defer t.lock.Unlock()
			t.errs = append(t.errs, err)
		}
		t.wg.Done()
	}()
	return ready, nil
}

func (t *services) Wait() error {
	t.wg.Wait()
	return errors.Join(t.errs...)
}

func (t *services) WaitGroup() *sync.WaitGroup {
	return t.wg
}
