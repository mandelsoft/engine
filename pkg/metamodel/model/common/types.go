package common

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Phase string
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

type Element interface {
	Id() ElementId
	GetObject() InternalObject
}

type Request struct {
	Element Element
}

type Status struct {
	Error error
}

type Object interface {
	database.Object

	database.GenerationAccess
}

type ExternalObject interface {
	Object
}

type RunAwareObject interface {
	ExternalObject

	GetRunId() RunId
}

type State interface {
	GetLinks(phase Phase) []ElementId
}

type InternalObject interface {
	Object
	GetState() State
	GetTargetState() State

	ClearLock(Phase)
	GetLock(Phase) RunId
	TryLock(Phase, RunId) bool

	Process(Request) Status
}

////////////////////////////////////////////////////////////////////////////////

type Objectbase = database.Database[Object]
