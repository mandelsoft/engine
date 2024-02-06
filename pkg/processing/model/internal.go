package model

import (
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Object = internal.Object
type InternalObject = internal.InternalObject
type ExternalObject = internal.ExternalObject
type NamespaceObject = internal.NamespaceObject

type CommitInfo = internal.CommitInfo
type Request = internal.Request
type CurrentState = internal.CurrentState
type TargetState = internal.TargetState
type ExternalState = internal.ExternalState
type ExternalStates = internal.ExternalStates
type OutputState = internal.OutputState
type ProcessingREsult = internal.ProcessingResult
type Creation = internal.Creation
type StatusUpdate = internal.StatusUpdate
type Status = internal.Status
type Inputs = internal.Inputs

type Logging = internal.Logging

const STATUS_INITIAL = Status("")
const STATUS_PENDING = Status("Pending")
const STATUS_BLOCKED = Status("Blocked")
const STATUS_PREPARING = Status("Preparing")
const STATUS_PROCESSING = Status("Processing")
const STATUS_WAITING = Status("Waiting")
const STATUS_COMPLETED = Status("Completed")
const STATUS_FAILED = Status("Failed")
const STATUS_DELETED = Status("Deleted")

func StatusFailed(err error) ProcessingREsult {
	return ProcessingREsult{
		Status: STATUS_FAILED,
		Error:  err,
	}
}

func StatusCompleted(result OutputState, err ...error) ProcessingREsult {
	return ProcessingREsult{
		Status:      STATUS_COMPLETED,
		ResultState: result,
		Error:       utils.Optional(err...),
	}
}

func StatusDeleted() ProcessingREsult {
	return ProcessingREsult{
		Status: STATUS_DELETED,
	}
}

func StatusCompletedWithCreation(creation []Creation, result OutputState, err ...error) ProcessingREsult {
	return ProcessingREsult{
		Status:      STATUS_COMPLETED,
		Creation:    creation,
		ResultState: result,
		Error:       utils.Optional(err...),
	}
}
