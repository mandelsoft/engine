package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type Object interface {
	database.Object

	database.GenerationAccess

	GetFinalizers() []string
	AddFinalizer(ob Objectbase, f string) (bool, error)
	RemoveFinalizer(ob Objectbase, f string) (bool, error)
	HasFinalizer(f string) bool

	IsDeleting() bool
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

	MarkPhasesForDeletion(ob Objectbase, phases ...Phase) (bool, error)
	IsMarkedForDeletion(phase Phase) bool

	AcceptExternalState(lctx Logging, ob Objectbase, ph Phase, ext ExternalStates) (AcceptStatus, error)
	Process(Request) ProcessingResult
	PrepareDeletion(lctx Logging, ob Objectbase, phase Phase) error
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
	// InputVersion version is the version of the inputs used for this commit.
	InputVersion string
	// ObjectVersion is an optional modified object version, which should be
	// uses instead of the onw from the target state.
	// It typically results from a modification of the external object
	// used as slave object.
	ObjectVersion *string
	OutputState   OutputState
}

////////////////////////////////////////////////////////////////////////////////

// SlaveCheckFunction is used to check the validity of an existing
// internal object to be usable as a slave.
type SlaveCheckFunction func(i InternalObject) error

// SlaveUpdateFunction is used to create or update a new internal slave object
// to be usable for the actual desired purpose.
// If created, it MUST prepare it with the correct locked runid
// and to provide the correct dependencies (at least for its
// target state, it must be able to work with a non-existent external object).
type SlaveUpdateFunction func(ob Objectbase, eid ElementId, i InternalObject) (created InternalObject, err error)

type SlaveManagement interface {
	AssureSlaves(check SlaveCheckFunction, update SlaveUpdateFunction, eids ...ElementId) error
}

type Request struct {
	Logging         Logging
	Model           ProcessingModel
	Delete          bool
	Inputs          Inputs
	Element         Element
	ElementAccess   ElementAccess
	SlaveManagement SlaveManagement
}

type Creation struct {
	Internal InternalObject
	Phase    Phase
}

type Status string

type ProcessingResult struct {
	Status                 Status
	ResultState            OutputState
	EffectiveObjectVersion *string
	Error                  error
}

func (s ProcessingResult) ModifyObjectVersion(v *string) ProcessingResult {
	s.EffectiveObjectVersion = v
	return s
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
