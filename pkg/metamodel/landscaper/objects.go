package landscaper

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

const TYPE_DATAOBJECT = "DO"
const TYPE_INSTALLATION = "I"
const TYPE_EXECUTION = "E"

const TYPE_DATAOBJECT_STATE = "SDO"
const TYPE_INSTALLATION_STATE = "SI"
const TYPE_EXECUTION_STATE = "SE"

const TYPE_NAMESPACE = "NS"

type Dependencies interface {
	GetLinks() []database.ObjectId
	GetVersion() string
}

type ExternalObject interface {
	database.Object
	Dependencies
}

type DataObject interface {
	ExternalObject
}

type Installation interface {
	ExternalObject
}

type Execution interface {
	ExternalObject
}

type InternalObject[E ExternalObject] interface {
	database.Object
	Dependencies

	GetActualVersion() string
	GetTargetVersion() string
	SetActualVersion(string)
	SetTargetVersion(string)
	SetTargetState(E) error

	Lock(metamodel.RunId) (bool, error)
	Unlock() error
}

type DataObjectState interface {
	InternalObject[DataObject]
}

type InstallationState interface {
	InternalObject[Installation]
}

type ExecutionState interface {
	InternalObject[Execution]
}

const NS_PHASE_LOCKING = "locking"
const NS_PHASE_READY = "ready"

type Namespace interface {
	database.Object

	SetPhaseLocking(metamodel.RunId) (bool, error)
	SetPhaseReady() error
}
