package processor

import (
	"context"
	"fmt"
	"sync"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/utils"
)

type PendingCounter struct {
	lock    sync.Mutex
	pending int64
	waiting []chan struct{}
}

func (p *PendingCounter) Add(delta int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.pending += delta

	fmt.Printf("******* pending %d *******\n", p.pending)
	if p.pending <= 0 {
		p.pending = 0
		for _, w := range p.waiting {
			close(w)
		}
		p.waiting = nil
	}
}

func (p *PendingCounter) Wait(ctx context.Context) bool {
	p.lock.Lock()

	c := make(chan struct{})
	p.waiting = append(p.waiting, c)
	p.lock.Unlock()

	defer func() {
		p.lock.Lock()
		for i, w := range p.waiting {
			if w == c {
				p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
				break
			}
		}
		p.lock.Unlock()
	}()
	select {
	case <-c:
		return true
	case <-ctx.Done():
		return false
	}
}

////////////////////////////////////////////////////////////////////////////////

type Future interface {
	Wait(ctx context.Context) bool
	FinalWait(ctx context.Context) bool
}

type future struct {
	retrigger bool
	lock      sync.Mutex
	done      int
	waiting   chan struct{}
}

func NewFuture(retrigger bool) *future {
	f := &future{retrigger: retrigger}
	return f
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
		f.retrigger = utils.Optional(retrigger...)
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
		fmt.Printf("*** TRIGGER ***\n")
		f.done++
	}
	return f.retrigger
}

////////////////////////////////////////////////////////////////////////////////

type EventManager struct {
	lock    sync.Mutex
	waiting map[ElementId][]*future
}

func NewEventManager() *EventManager {
	return &EventManager{
		waiting: map[ElementId][]*future{},
	}
}

func (p *EventManager) Completed(id ElementId) {
	p.lock.Lock()
	defer p.lock.Unlock()

	waiting := p.waiting[id]
	if waiting != nil {
		var n []*future
		for _, w := range waiting {
			if w.retrigger {
				n = append(n, w)
			}
			w.Trigger()
		}
		p.waiting[id] = n
	}
}

func (p *EventManager) Future(id ElementId, retrigger ...bool) Future {
	p.lock.Lock()
	defer p.lock.Unlock()

	f := NewFuture(utils.Optional(retrigger...))
	p.waiting[id] = append(p.waiting[id], f)
	return f
}

func (p *EventManager) Wait(ctx context.Context, id ElementId) bool {
	return p.Future(id).Wait(ctx)
}
