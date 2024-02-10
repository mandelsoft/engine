package internal

import (
	"io"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type ExternalObjectType interface {
	Name() string
	Trigger() ElementType
	IsForeignControlled() bool
}

type InternalObjectType interface {
	Name() string

	Phases() []Phase
	Element(Phase) ElementType
}

type ElementType interface {
	Id() TypeId

	Dependencies() []ElementType
	TriggeredBy() []string
	AssignedExternalStates() []string
	HasDependency(name TypeId) bool
}

type MetaModel interface {
	Name() string

	NamespaceType() string
	InternalTypes() []string
	Phases(objtype string) []Phase
	ExternalTypes() []string
	ElementTypes() []TypeId

	IsExternalType(name string) bool
	IsInternalType(name string) bool
	HasElementType(name TypeId) bool

	GetExternalType(name string) ExternalObjectType
	GetInternalType(name string) InternalObjectType
	GetElementType(name TypeId) ElementType
	HasDependency(s, d TypeId) bool

	GetDependentTypePhases(name TypeId) []Phase
	GetPhaseFor(ext string) *TypeId
	GetTriggeringTypesForElementType(id TypeId) []string
	GetAssignedExternalTypes(id TypeId) []string
	GetTriggeringTypesForInternalType(name string) []string

	Dump(w io.Writer)
}
