package locks

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"
)

type lockState struct {
	waiting []block
}

type block chan struct{}

type ElementLocks[T comparable] struct {
	lock  sync.Mutex
	locks map[T]*lockState
}

func NewElementLocks[T comparable]() *ElementLocks[T] {
	return &ElementLocks[T]{locks: map[T]*lockState{}}
}

func (e *ElementLocks[T]) IsLocked(eid T) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.locks[eid] != nil
}

func (e *ElementLocks[T]) HasWaiting(eid T) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.locks[eid] != nil && len(e.locks[eid].waiting) > 0
}

func (e *ElementLocks[T]) TryLock(eid T) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	if locked := e.locks[eid]; locked != nil {
		return false
	}
	e.locks[eid] = &lockState{}
	return true
}

func (e *ElementLocks[T]) Unlock(eid T) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if locked := e.locks[eid]; locked != nil {
		if len(locked.waiting) > 0 {
			locked.waiting[0] <- struct{}{}
			locked.waiting = locked.waiting[1:]
		} else {
			delete(e.locks, eid)
		}
	} else {
		panic(fmt.Sprintf("unlocking unlocked element %v", eid))
	}
}

func (e *ElementLocks[T]) Lock(ctx context.Context, eid T) error {
	e.lock.Lock()

	if locked := e.locks[eid]; locked != nil {
		block := make(block)
		locked.waiting = append(locked.waiting, block)
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
		e.locks[eid] = &lockState{}
		e.lock.Unlock()
		return nil
	}
}
