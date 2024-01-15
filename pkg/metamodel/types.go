package metamodel

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Encoding = runtime.Encoding[Object]
type Scheme = runtime.Scheme[Object]

func NewScheme() Scheme {
	return runtime.NewYAMLScheme[Object]()
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s Scheme) {
	runtime.Register[T, P](s, runtime.TypeOf[T]().Name())
}

type RunId string

type Phase string

type Request struct {
	Element Element
}

type Status struct {
	Error error
}

type Object interface {
	database.Object

	Process(Request) Status
}

type RunAwareObject interface {
	Object

	GetRunId() RunId
}

const DEFAULT_PHASE = Phase("PhaseUpdating")

type TypeSpecification struct {
	Name string
}

type InternalTypeSpecification struct {
	TypeSpecification
	Phases []Phase
}

func IntSpec(tname string, phases ...Phase) InternalTypeSpecification {
	if len(phases) == 0 {
		phases = []Phase{DEFAULT_PHASE}
	}
	return InternalTypeSpecification{
		TypeSpecification: TypeSpecification{tname},
		Phases:            slices.Clone(phases),
	}
}

type DependencyType struct {
	Type  string
	Phase Phase
}

func Dep(typ string, phase Phase) DependencyType {
	return DependencyType{typ, phase}
}

type ExternalTypeSpecification struct {
	TypeSpecification

	Trigger      DependencyType
	Dependencies []DependencyType
}

func ExtSpec(tname string, inttype string, phase Phase, dependencies ...DependencyType) ExternalTypeSpecification {
	if phase == "" {
		phase = DEFAULT_PHASE
	}
	return ExternalTypeSpecification{
		TypeSpecification: TypeSpecification{tname},
		Trigger: DependencyType{
			Type:  inttype,
			Phase: phase,
		},
		Dependencies: slices.Clone(dependencies),
	}
}

type MetaModel interface {
	GetEncoding() Encoding
	GetTypes() (external []ExternalTypeSpecification, internal []InternalTypeSpecification)
}

type State interface {
	GetDependencies() []Element
	GetVersion() string
}

type Element interface {
	GetName() string

	GetPhase() Phase

	GetObject() Object

	GetCurrentState() State
	GetTargetState() State
}
