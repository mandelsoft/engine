package db

import (
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

type InternalDBObject interface {
	Object
}

type InternalDBObjectSupport struct {
	ObjectMeta
}

////////////////////////////////////////////////////////////////////////////////

// PhaseState is the generic interface for
// the state information of a phase stored
// in a state object.
// TODO: provide type parameters for actual type of current and target state.
type PhaseState interface {
	ClearLock(mmids.RunId) bool
	GetLock() mmids.RunId
	TryLock(mmids.RunId) bool

	GetStatus() model.Status
	SetStatus(model.Status) bool
	GetCurrent() CurrentState
	ClearTarget() bool
	GetTarget() TargetState
	CreateTarget() TargetState

	MarkForDeletion(t utils.Timestamp) bool
	IsDeletionRequested() bool
}

type CommonState interface {
	GetObjectVersion() string
	SetObjectVersion(string) bool
}

type CurrentState interface {
	CommonState

	GetFormalVersion() string
	SetFormalVersion(v string) bool

	GetObservedVersion() string
	SetObservedVersion(v string) bool

	GetInputVersion() string
	SetInputVersion(string) bool

	GetOutputVersion() string
	SetOutputVersion(string) bool
}

type TargetState interface {
	CommonState

	GetFormalObjectVersion() string
	SetFormalObjectVersion(v string) bool
}

////////////////////////////////////////////////////////////////////////////////
// Generic phase state persistence
////////////////////////////////////////////////////////////////////////////////

type targetpointer[P any] interface {
	TargetState
	*P
}

type currentpointer[P any] interface {
	CurrentState
	*P
}

// DefaultPhaseState is a set of fields completely
// describing the persistent state of a phase
// for a state object.
// It also covers phase specific state by requiring
// types for the cirrent and target state implementing
// the required standard state.
// Such types can be composed by embedding the
// the standard states [StandardCurrentState] and
// [StandardTargetState], respectively.
// Type parameters are required for the struct and the
// pointer type.
type DefaultPhaseState[C any, T any, CP currentpointer[C], TP targetpointer[T]] struct {
	RunId             mmids.RunId      `json:"runid"`
	Status            model.Status     `json:"status"`
	DeletionRequested *utils.Timestamp `json:"deletionRequested,omitempty"`
	Current           C                `json:"current,omitempty"`
	Target            TP               `json:"target,omitempty"`
}

var _ PhaseState = (*DefaultPhaseState[StandardCurrentState, StandardTargetState, *StandardCurrentState, *StandardTargetState])(nil)

func (n *DefaultPhaseState[C, T, CP, TP]) ClearLock(id mmids.RunId) bool {
	if n.RunId != id {
		return false
	}
	n.RunId = ""
	return true
}

func (n *DefaultPhaseState[C, T, CP, TP]) GetLock() mmids.RunId {
	return n.RunId
}

func (n *DefaultPhaseState[C, T, CP, TP]) TryLock(id mmids.RunId) bool {
	if n.RunId != "" && n.RunId != id {
		return false
	}
	n.RunId = id
	return true
}

func (n *DefaultPhaseState[C, T, CP, TP]) GetStatus() model.Status {
	if n.RunId != "" {
		if n.Status == model.STATUS_COMPLETED || n.Status == model.STATUS_FAILED {
			return model.STATUS_PENDING
		}
	}
	return n.Status
}

func (n *DefaultPhaseState[C, T, CP, TP]) SetStatus(s model.Status) bool {
	if n.Status == s {
		return false
	}
	n.Status = s
	return true
}

func (n *DefaultPhaseState[C, T, CP, TP]) GetCurrent() CurrentState {
	return CP(&n.Current)
}

func (n *DefaultPhaseState[C, T, CP, TP]) GetTarget() TargetState {
	return n.Target
}

func (n *DefaultPhaseState[C, T, CP, TP]) CreateTarget() TargetState {
	if n.Target == nil {
		var t T
		n.Target = &t
	}
	return n.Target
}

func (n *DefaultPhaseState[C, T, CP, TP]) ClearTarget() bool {
	if n.Target == nil {
		return false
	}
	n.Target = nil
	return true
}

func (n *DefaultPhaseState[C, T, CP, TP]) MarkForDeletion(t utils.Timestamp) bool {
	if n.DeletionRequested != nil {
		return false
	}
	n.DeletionRequested = &t
	return true
}

func (n *DefaultPhaseState[C, T, CP, TP]) IsDeletionRequested() bool {
	return n.DeletionRequested != nil
}
