package common

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/logging"
)

type Phase string
type Encoding = database.Encoding[Object]
type SchemeTypes = database.SchemeTypes[Object]
type Scheme = database.Scheme[Object]

type Logging = logging.AttributionContext

func NewScheme() Scheme {
	return runtime.NewYAMLScheme[Object]().(Scheme) // Goland
}

type RunId string

// MetaModel provides basic access function required by
// models.
// The full type is defined in package metamodel,
// which cannot be used, here, to void package cycles.
type MetaModel interface {
	Name() string
	NamespaceType() string
	ExternalTypes() []string
	InternalTypes() []string

	IsExternalType(name string) bool
	IsInternalType(name string) bool

	Phases(ityp string) []Phase
	HasElementType(name TypeId) bool
	HasDependency(s, d TypeId) bool
	GetDependentTypePhases(name TypeId) []Phase
	GetPhaseFor(ext string) *TypeId
	GetTriggeringTypesForElementType(id TypeId) []string
	GetTriggeringTypesForInternalType(name string) []string
}

type Namespace interface {
	GetNamespaceName() string
	GetElement(id ElementId) Element
}

type NameSource interface {
	GetName() string
	GetNamespace() string
}

type Element interface {
	NameSource

	Id() ElementId
	GetType() string
	GetPhase() Phase
	GetObject() InternalObject

	GetLock() RunId
}

type OutputState interface {
	GetOutputVersion() string
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
	Status *ProcessingStatus
	// Message is an explaining text for the state.
	Message *string
	// ResultState is some state info provided by the internal object.
	ResultState OutputState
}

type ElementAccess interface {
	GetElement(ElementId) Element
}

type ProcessingModel interface {
	ObjectBase() Objectbase
	MetaModel() MetaModel
	SchemeTypes() SchemeTypes

	GetNamespace(name string) Namespace
	GetElement(id ElementId) Element
}

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

type ProcessingStatus string

const STATUS_WAITING = ProcessingStatus("Waiting")
const STATUS_PENDING = ProcessingStatus("Pending")
const STATUS_PREPARING = ProcessingStatus("Preparing")
const STATUS_PROCESSING = ProcessingStatus("Processing")
const STATUS_COMPLETED = ProcessingStatus("Completed")
const STATUS_FAILED = ProcessingStatus("Failed")

type Status struct {
	Status      ProcessingStatus
	Creation    []Creation
	Deleted     bool
	ResultState OutputState
	Error       error
}

type Object interface {
	database.Object

	database.GenerationAccess
}

type ExternalObject interface {
	Object

	GetState() ExternalState

	UpdateStatus(lctx Logging, ob Objectbase, elem ElementId, update StatusUpdate) error
}

type RunAwareObject interface {
	ExternalObject

	GetRunId() RunId
}

type ExternalState interface {
	GetVersion() string
}

type ExternalStates map[string]ExternalState

type LinkState interface {
	GetLinks() []ElementId
}

type CurrentState interface {
	LinkState
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

type CommitInfo struct {
	InputVersion string
	State        OutputState
}

type InternalObject interface {
	Object

	GetCurrentState(phase Phase) CurrentState
	GetTargetState(phase Phase) TargetState

	GetLock(Phase) RunId
	TryLock(Objectbase, Phase, RunId) (bool, error)
	Rollback(lctx Logging, ob Objectbase, ph Phase, id RunId) (bool, error)
	Commit(lctx Logging, ob Objectbase, ph Phase, id RunId, atomic *CommitInfo) (bool, error)

	SetExternalState(lctx Logging, ob Objectbase, ph Phase, ext ExternalStates) error
	Process(Request) Status
}

////////////////////////////////////////////////////////////////////////////////

type Objectbase interface {
	database.Database[Object]
	CreateObject(database.ObjectId) (Object, error)
}
