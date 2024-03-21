package model

import (
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/general"
)

type Object = internal.Object
type InternalObject = internal.InternalObject
type ExternalObject = internal.ExternalObject
type NamespaceObject = internal.NamespaceObject
type UpdateRequestObject = internal.UpdateRequestObject
type UpdateAction = internal.UpdateAction
type ElementRef = internal.ElementRef
type UpdateStatus = internal.UpdateStatus

type CommitInfo = internal.CommitInfo
type SlaveCheckFunction = internal.SlaveCheckFunction
type SlaveUpdateFunction = internal.SlaveUpdateFunction
type ExternalUpdateFunction = internal.ExternalUpdateFunction
type SlaveManagement = internal.SlaveManagement
type Request = internal.Request
type LinkState = internal.LinkState
type ObservedState = internal.ObservedState
type CurrentState = internal.CurrentState
type TargetState = internal.TargetState
type AcceptStatus = internal.AcceptStatus
type ExternalState = internal.ExternalState
type OutputState = internal.OutputState
type ProcessingResult = internal.ProcessingResult
type Creation = internal.Creation
type StatusUpdate = internal.StatusUpdate
type StatusSource = internal.StatusSource
type Status = internal.Status
type Inputs = internal.Inputs

type Logging = internal.Logging

const (
	REQ_ACTION_ACQUIRE  = "Acquire"
	REQ_STATUS_ACQUIRED = "Acquired"

	REQ_ACTION_LOCK   = "Lock"
	REQ_STATUS_LOCKED = "Locked"

	REQ_ACTION_RELEASE  = "Release"
	REQ_STATUS_RELEASED = "Released"

	REQ_STATUS_PENDING = "Pending"
	REQ_STATUS_INVALID = "Invalid"
)

const (
	ACCEPT_OK AcceptStatus = iota
	ACCEPT_INVALID
)

const (
	STATUS_INITIAL    = Status("")
	STATUS_PENDING    = Status("Pending")
	STATUS_INVALID    = Status("Invalid")
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
		Error:       general.Optional(err...),
	}
}

func StatusDeleting(err ...error) ProcessingResult {
	return ProcessingResult{
		Status: STATUS_DELETING,
		Error:  general.Optional(err...),
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

func NewElementRef(typ, name string, phase mmids.Phase) ElementRef {
	return ElementRef{
		Name:  name,
		Type:  typ,
		Phase: phase,
	}
}
