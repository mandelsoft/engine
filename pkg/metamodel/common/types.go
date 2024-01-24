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

func NewScheme() Scheme {
	return runtime.NewYAMLScheme[Object]().(Scheme) // Goland
}

type RunId string

type Element interface {
	Id() ElementId
	GetType() string
	GetNamespace() string
	GetName() string
	GetPhase() Phase
	GetObject() InternalObject
}

type InternalState interface {
	Description() string
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
	Status *string
	// Message is an explaining text for the state.
	Message *string
	// InternalState is some state info provided by the internal object.
	InternalState InternalState
}

type Request struct {
	LogContext logging.Context
	External   []ObjectId
	Inputs     Inputs
	Element    Element
}

type Creation struct {
	Internal InternalObject
	Phase    Phase
}

type processingStatus string

const STATUS_COMPLETED = processingStatus("Completed")
const STATUS_PROCESSING = processingStatus("Processing")
const STATUS_FAILED = processingStatus("Failed")

type Status struct {
	Status        processingStatus
	Creation      []Creation
	InternalState InternalState
	Error         error
}

type Object interface {
	database.Object

	database.GenerationAccess
}

type ExternalObject interface {
	Object

	GetState() ExternalState

	UpdateStatus(ob Objectbase, elem ElementId, update StatusUpdate) error
}

type RunAwareObject interface {
	ExternalObject

	GetRunId() RunId
}

type ExternalState interface {
	GetVersion() string
}

type LinkState interface {
	GetLinks() []ElementId
}

type CurrentState interface {
	LinkState
	GetInputVersion() string
	GetObjectVersion() string
	GetOutputVersion() string
}

type Inputs = map[ElementId]CurrentState

type TargetState interface {
	LinkState
	GetObjectVersion() string

	GetInputVersion(Inputs) string
}

type InternalObject interface {
	Object

	GetCurrentState(phase Phase) CurrentState
	GetTargetState(phase Phase) TargetState

	GetLock(Phase) RunId

	ClearLock(Objectbase, Phase, RunId) (bool, error)
	TryLock(Objectbase, Phase, RunId) (bool, error)

	SetExternalState(ob Objectbase, ph Phase, typ string, ext ExternalState) error

	Process(Objectbase, Request) Status
}

////////////////////////////////////////////////////////////////////////////////

type Objectbase interface {
	database.Database[Object]
	CreateObject(ObjectId) (Object, error)
}
