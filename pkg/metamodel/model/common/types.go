package common

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
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
	GetObject() *InternalObject
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

	GetState() State
}

type RunAwareObject interface {
	ExternalObject

	GetRunId() RunId
}

type State interface {
	GetLinks() []ElementId
	GetVersion() string
}

type InternalObject interface {
	Object

	GetState(phase Phase) State
	GetTargetState(phase Phase) State

	ClearLock(Phase)
	GetLock(Phase) RunId
	TryLock(Phase, RunId) bool

	Process(Request) Status
}

////////////////////////////////////////////////////////////////////////////////

type Objectbase = database.Database[Object]
