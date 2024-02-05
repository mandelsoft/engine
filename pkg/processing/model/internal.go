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
type Status = internal.Status
type Creation = internal.Creation
type StatusUpdate = internal.StatusUpdate
type ProcessingStatus = internal.ProcessingStatus
type Inputs = internal.Inputs

type Logging = internal.Logging

const STATUS_WAITING = ProcessingStatus("Waiting")
const STATUS_PENDING = ProcessingStatus("Pending")
const STATUS_PREPARING = ProcessingStatus("Preparing")
const STATUS_PROCESSING = ProcessingStatus("Processing")
const STATUS_COMPLETED = ProcessingStatus("Completed")
const STATUS_DELETED = ProcessingStatus("Deleted")
const STATUS_FAILED = ProcessingStatus("Failed")

func StatusFailed(err error) Status {
	return Status{
		Status: STATUS_FAILED,
		Error:  err,
	}
}

func StatusCompleted(result OutputState, err ...error) Status {
	return Status{
		Status:      STATUS_COMPLETED,
		ResultState: result,
		Error:       utils.Optional(err...),
	}
}

func StatusDeleted() Status {
	return Status{
		Status: STATUS_DELETED,
	}
}

func StatusCompletedWithCreation(creation []Creation, result OutputState, err ...error) Status {
	return Status{
		Status:      STATUS_COMPLETED,
		Creation:    creation,
		ResultState: result,
		Error:       utils.Optional(err...),
	}
}
