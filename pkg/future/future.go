package future

import (
	"context"
	"fmt"
	"sync"

	"github.com/mandelsoft/goutils/general"
)

////////////////////////////////////////////////////////////////////////////////

type Future interface {
	Wait(ctx context.Context) bool
	FinalWait(ctx context.Context) bool
}

type Trigger interface {
	Future
	Trigger() bool
}

type future struct {
	retrigger bool
	lock      sync.Mutex
	done      int
	waiting   chan struct{}
}

func NewFuture(retrigger ...bool) *future {
	return &future{retrigger: general.Optional(retrigger...)}
}

func (f *future) FinalWait(ctx context.Context) bool {
	return f.wait(ctx, false)
}

func (f *future) Wait(ctx context.Context) bool {
	return f.wait(ctx)
}

func (f *future) wait(ctx context.Context, retrigger ...bool) bool {
	f.lock.Lock()
	if len(retrigger) > 0 {
		f.retrigger = general.Optional(retrigger...)
	}
	if f.done > 0 {
		f.done--
		f.lock.Unlock()
		fmt.Printf("*** GOT ***\n")
		return true
	}

	if f.waiting == nil {
		f.waiting = make(chan struct{})
	}

	wait := f.waiting
	f.lock.Unlock()

	fmt.Printf("*** WAITING ***\n")
	select {
	case <-wait:
		fmt.Printf("*** WAIT DONE ***\n")
		return true
	case <-ctx.Done():
		return false
	}
}

func (f *future) Trigger() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.waiting != nil {
		wait := f.waiting
		if f.retrigger {
			f.waiting = nil
		}
		fmt.Printf("*** TRIGGER WAITING ***\n")
		close(wait)
	} else {
		fmt.Printf("*** TRIGGER COUNT ***\n")
		f.done++
	}
	return f.retrigger
}
