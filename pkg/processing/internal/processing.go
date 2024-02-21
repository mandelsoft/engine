package internal

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type Namespace interface {
	GetNamespaceName() string
	Elements() []ElementId
	GetElement(id ElementId) Element
}

type Element interface {
	NameSource

	Id() ElementId
	GetType() string
	GetPhase() Phase
	GetObject() InternalObject

	GetLock() RunId

	GetExternalState(ExternalObject) ExternalState

	GetStatus() Status

	MarkForDeletion(m ProcessingModel) (changed bool, all []Phase, leafs []Phase, err error)
	IsMarkedForDeletion() bool
}

type ElementAccess interface {
	GetElement(ElementId) Element
}

type ProcessingModel interface {
	ObjectBase() Objectbase
	MetaModel() MetaModel
	SchemeTypes() SchemeTypes
	Namespaces() []string
	GetNamespace(name string) Namespace
	GetElement(id ElementId) Element
}
