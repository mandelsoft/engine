package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

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
	Rollback(lctx Logging, ob Objectbase, ph Phase, id RunId, tgt TargetState, formal *string) (bool, error)
	Commit(lctx Logging, ob Objectbase, ph Phase, id RunId, atomic *CommitInfo) (bool, error)

	GetStatus(Phase) Status
	SetStatus(ob Objectbase, phase Phase, status Status) (bool, error)

	MarkPhasesForDeletion(ob Objectbase, phases ...Phase) (bool, error)
	IsMarkedForDeletion(phase Phase) bool

	AcceptExternalState(lctx Logging, ob Objectbase, ph Phase, ext ExternalState) (AcceptStatus, error)
	Process(Request) ProcessingResult
	PrepareDeletion(lctx Logging, mgmt SlaveManagement, phase Phase) error
}

////////////////////////////////////////////////////////////////////////////////

type ExternalState interface {
	GetVersion() string
}

type LinkState interface {
	GetLinks() []ElementId
}

type ObservedState interface {
	LinkState
	GetObjectVersion() string
}

type CurrentState interface {
	GetObservedState() ObservedState

	LinkState
	GetFormalVersion() string
	GetInputVersion() string
	GetObjectVersion() string
	GetOutputVersion() string

	GetOutput() OutputState
}

type Inputs = map[ElementId]OutputState

type TargetState interface {
	LinkState
	GetObjectVersion() string
	GetFormalObjectVersion() string

	GetInputVersion(Inputs) string
}

type OutputState interface {
	GetFormalVersion() string
	GetOutputVersion() string
}

type CommitInfo struct {
	// FormalVersion is the formal (graph) version, which is committed.
	FormalVersion string
	// InputVersion is the version of the inputs used for this commit.
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

// ExternalCheckFunction is used to check the validity of an existing
// external object to be usable as a slave.
type ExternalCheckFunction func(e ExternalObject) error

// ExternalUpdateFunction is used to create or update a new external slave object
type ExternalUpdateFunction func(ob Objectbase, oid database.ObjectId, e ExternalObject) (bool, ExternalObject, error)

type SlaveManagement interface {
	AssureSlaves(check SlaveCheckFunction, update SlaveUpdateFunction, eids ...ElementId) error
	MarkForDeletion(eids ...ElementId) error

	AssureExternal(update ExternalUpdateFunction, oid database.ObjectId) (bool, ExternalObject, error)

	ObjectBase() Objectbase
}

type Request struct {
	Logging         Logging
	Model           ProcessingModel
	Delete          bool
	Inputs          Inputs
	FormalVersion   string
	Element         Element
	ElementAccess   ElementAccess
	SlaveManagement SlaveManagement
}

type Creation struct {
	Internal InternalObject
	Phase    Phase
}

type StatusSource interface {
	GetStatus() Status
}

type Status string

func (s Status) GetStatus() Status {
	return s
}

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
	// FormalVersion is the formal version reached.
	FormalVersion *string
	// DetectedVersion is the object version currently detected by the system.
	DetectedVersion *string
	// ObservedVersion is the object version reached.
	ObservedVersion *string
	// EffectiveVersion is the graph version reached.
	EffectiveVersion *string
	// Status is a state value.
	Status *Status
	// Message is an explaining text for the state.
	Message *string
	// ExternalState as provided by state object for external object
	ExternalState ExternalState
	// ResultState is some state info provided by the internal object.
	ResultState OutputState
}
