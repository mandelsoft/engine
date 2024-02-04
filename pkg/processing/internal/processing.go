package internal

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type Namespace interface {
	GetNamespaceName() string
	GetElement(id ElementId) Element
}

type Element interface {
	NameSource

	Id() ElementId
	GetType() string
	GetPhase() Phase
	GetObject() InternalObject

	GetLock() RunId
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
