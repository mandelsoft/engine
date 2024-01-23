package model

import (
	"github.com/google/uuid"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
)

type Scheme = common.Scheme
type SchemeTypes = common.SchemeTypes
type Object = common.Object
type InternalObject = common.InternalObject
type ExternalObject = common.ExternalObject
type RunId = common.RunId
type Phase = common.Phase
type State = common.State
type Request = common.Request
type Status = common.Status

func NewRunId() RunId {
	return RunId(uuid.New().String())
}
