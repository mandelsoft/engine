package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type Object interface {
	database.Object

	database.GenerationAccess
}

type NamespaceObject interface {
	Object

	// GetNamespaceName provides the effective (hierarchical) namespace
	// name, while GetName() provides the name of the namespace object in
	// its namespace.
	GetNamespaceName() string

	GetLock() RunId

	ClearLock(ob Objectbase, id RunId) (bool, error)
	TryLock(db Objectbase, id RunId) (bool, error)
}

type ExternalObject interface {
	Object

	GetState() ExternalState

	UpdateStatus(lctx Logging, ob Objectbase, elem ElementId, update StatusUpdate) error
}

type AcceptStatus int

type InternalObject interface {
	Object

	GetExternalState(o ExternalObject, phase Phase) ExternalState

	GetCurrentState(phase Phase) CurrentState
	GetTargetState(phase Phase) TargetState

	GetLock(Phase) RunId
	TryLock(Objectbase, Phase, RunId) (bool, error)
	Rollback(lctx Logging, ob Objectbase, ph Phase, id RunId, observed ...string) (bool, error)
	Commit(lctx Logging, ob Objectbase, ph Phase, id RunId, atomic *CommitInfo) (bool, error)

	GetStatus(Phase) Status
	SetStatus(ob Objectbase, phase Phase, status Status) (bool, error)

	AcceptExternalState(lctx Logging, ob Objectbase, ph Phase, ext ExternalStates) (AcceptStatus, error)
	Process(Request) ProcessingResult
}

////////////////////////////////////////////////////////////////////////////////

type ExternalState interface {
	GetVersion() string
}

type ExternalStates map[string]ExternalState

type LinkState interface {
	GetLinks() []ElementId
}

type CurrentState interface {
	LinkState
	GetObservedVersion() string
	GetInputVersion() string
	GetObjectVersion() string
	GetOutputVersion() string

	GetOutput() OutputState
}

type Inputs = map[ElementId]OutputState

type TargetState interface {
	LinkState
	GetObjectVersion() string

	GetInputVersion(Inputs) string
}

type OutputState interface {
	GetOutputVersion() string
}

type CommitInfo struct {
	InputVersion string
	OutputState  OutputState
}

////////////////////////////////////////////////////////////////////////////////

type Request struct {
	Logging       Logging
	Model         ProcessingModel
	Inputs        Inputs
	Element       Element
	ElementAccess ElementAccess
}

type Creation struct {
	Internal InternalObject
	Phase    Phase
}

type Status string

type ProcessingResult struct {
	Status      Status
	Creation    []Creation
	ResultState OutputState
	Error       error
}

type StatusUpdate struct {
	// RunId is the id of the actually pending Run.
	RunId *RunId
	// DetectedVersion is object version currently detected by the system.
	DetectedVersion *string
	// ObservedVersion is the object version reached.
	ObservedVersion *string
	// EffectiveVersion is the graph version reached.
	EffectiveVersion *string
	// Status is a state value.
	Status *Status
	// Message is an explaining text for the state.
	Message *string
	// ResultState is some state info provided by the internal object.
	ResultState OutputState
}
