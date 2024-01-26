package processing

import (
	"context"
	"fmt"
	"sync"
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

type EventManager struct {
	lock    sync.Mutex
	waiting map[ElementId][]chan struct{}
}

func NewEventManager() *EventManager {
	return &EventManager{
		waiting: map[ElementId][]chan struct{}{},
	}
}

func (p *EventManager) Completed(id ElementId) {
	p.lock.Lock()
	defer p.lock.Unlock()

	waiting := p.waiting[id]
	if waiting != nil {
		for _, w := range waiting {
			close(w)
		}
		p.waiting[id] = nil
	}
}

func (p *EventManager) Wait(ctx context.Context, id ElementId) bool {
	p.lock.Lock()

	c := make(chan struct{})
	p.waiting[id] = append(p.waiting[id], c)
	p.lock.Unlock()

	defer func() {
		p.lock.Lock()
		waiting := p.waiting[id]
		for i, w := range waiting {
			if w == c {
				p.waiting[id] = append(waiting[:i], waiting[i+1:]...)
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
