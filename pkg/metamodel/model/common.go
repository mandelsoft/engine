package model

import (
	"github.com/google/uuid"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
)

type Scheme = common.Scheme
type SchemeTypes = common.SchemeTypes
type Object = common.Object
type CommitInfo = common.CommitInfo
type InternalObject = common.InternalObject
type ExternalObject = common.ExternalObject
type RunId = common.RunId
type Phase = common.Phase

type LinkState = common.LinkState
type ExternalState = common.ExternalState
type CurrentState = common.CurrentState
type TargetState = common.TargetState
type Inputs = common.Inputs

type OutputState = common.OutputState
type StatusUpdate = common.StatusUpdate

type Request = common.Request
type Status = common.Status

type Encoding = common.Encoding
type ElementId = common.ElementId
type ObjectId = common.ObjectId
type TypeId = common.TypeId

func NewRunId() RunId {
	return RunId(uuid.New().String())
}
