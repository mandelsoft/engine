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
type SlaveCheckFunction = internal.SlaveCheckFunction
type SlaveUpdateFunction = internal.SlaveUpdateFunction
type SlaveManagement = internal.SlaveManagement
type Request = internal.Request
type LinkState = internal.LinkState
type CurrentState = internal.CurrentState
type TargetState = internal.TargetState
type AcceptStatus = internal.AcceptStatus
type ExternalState = internal.ExternalState
type ExternalStates = internal.ExternalStates
type OutputState = internal.OutputState
type ProcessingResult = internal.ProcessingResult
type Creation = internal.Creation
type StatusUpdate = internal.StatusUpdate
type Status = internal.Status
type Inputs = internal.Inputs

type Logging = internal.Logging

const (
	ACCEPT_OK = iota
	ACCEPT_REJECTED
	ACCEPT_INVALID
)

const (
	STATUS_INITIAL    = Status("")
	STATUS_PENDING    = Status("Pending")
	STATUS_BLOCKED    = Status("Blocked")
	STATUS_PREPARING  = Status("Preparing")
	STATUS_PROCESSING = Status("Processing")
	STATUS_DELETING   = Status("Deleting")
	STATUS_WAITING    = Status("Waiting")
	STATUS_COMPLETED  = Status("Completed")
	STATUS_FAILED     = Status("Failed")
	STATUS_DELETED    = Status("Deleted")
)

func StatusFailed(err error) ProcessingResult {
	return ProcessingResult{
		Status: STATUS_FAILED,
		Error:  err,
	}
}

func StatusCompleted(result OutputState, err ...error) ProcessingResult {
	return ProcessingResult{
		Status:      STATUS_COMPLETED,
		ResultState: result,
		Error:       utils.Optional(err...),
	}
}

func StatusDeleted() ProcessingResult {
	return ProcessingResult{
		Status: STATUS_DELETED,
	}
}

func StatusWaiting() ProcessingResult {
	return ProcessingResult{
		Status: STATUS_WAITING,
	}
}
