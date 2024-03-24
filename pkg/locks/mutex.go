package locks

import (
	"sync"

	"golang.org/x/net/context"
)

type Mutex struct {
	lock    sync.Mutex
	locked  bool
	waiting []block
}

func (e *Mutex) IsLocked() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.locked
}

func (e *Mutex) HasWaiting() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return len(e.waiting) > 0
}

func (e *Mutex) TryLock() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.locked {
		return false
	}
	e.locked = true
	return true
}

func (e *Mutex) Unlock() {
	e.lock.Lock()
	defer e.lock.Unlock()

	if !e.locked {
		panic("unlocking unlocked mutex")
	}

	if len(e.waiting) > 0 {
		e.waiting[0] <- struct{}{}
		e.waiting = e.waiting[1:]
	} else {
		e.locked = false
	}
}

func (e *Mutex) Lock(ctx context.Context) error {
	e.lock.Lock()

	if e.locked {
		block := make(block)
		e.waiting = append(e.waiting, block)
		e.lock.Unlock()
		if ctx == nil {
			<-block
			return nil
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-block:
				return nil
			}
		}
	} else {
		e.locked = true
		e.lock.Unlock()
		return nil
	}
}
