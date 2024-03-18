package pool

import (
	"time"

	"github.com/mandelsoft/goutils/general"
)

// Status Interface is the interface which actions have to implement .
// Contract:
// Completed  Error
//  true,      nil: valid resources, everything ok, just continue normally
//  true,      err: valid resources, but resources not ready yet (required state for reconciliation/deletion not yet) reached, re-add to the queue rate-limited
//  false,     nil: valid resources, but reconciliation failed temporarily, just re-add to the queue
//  false,     err: invalid resources (not suitable for controller)

type Status struct {
	Completed bool
	Error     error

	// Interval selects a modified reconcilation reschedule for the actual item
	// -1 (default) no modification
	//  0 no reschedule
	//  >0 rescgule after given interval
	// If multiple reconcilers are called for an item the Intervals are combined as follows.
	// - if there is at least one status with Interval> 0,the minimum is used
	// - if all status disable reschedule it will be disabled
	// - status with -1 are ignored
	Interval time.Duration
}

func (s Status) IsSucceeded() bool {
	return s.Completed && s.Error == nil
}

func (s Status) IsDelayed() bool {
	return s.Completed && s.Error != nil
}

func (s Status) IsFailed() bool {
	return !s.Completed && s.Error != nil
}

func (s Status) MustBeRepeated() bool {
	return !s.Completed && s.Error == nil
}

func (s Status) RescheduleAfter(d time.Duration) Status {
	if s.Interval < 0 || d < s.Interval {
		s.Interval = d
	}
	return s
}

func (s Status) Stop() Status {
	s.Interval = 0
	return s
}

func (s Status) StopIfSucceeded() Status {
	if s.IsSucceeded() {
		s.Interval = 0
	}
	return s
}

func StatusCompleted(err ...error) Status {
	return Status{Completed: true, Error: general.Optional(err...), Interval: -1}
}

func StatusFailed(err error) Status {
	return Status{Completed: false, Error: err, Interval: -1}
}

func StatusRedo() Status {
	return Status{Completed: false, Error: nil, Interval: -1}
}
