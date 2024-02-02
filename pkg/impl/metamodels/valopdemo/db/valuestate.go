package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[ValueState, support.DBObject](Scheme) // Goland requires second type parameter
}

type ValueState struct {
	support.InternalDBObjectSupport `json:",inline"`

	Current CurrentState `json:"current"`
	Target  *TargetState `json:"target,omitempty"`
}

var _ support.InternalDBObject = (*ValueState)(nil)

type CurrentState struct {
	Owner         string      `json:"owner,omitempty"`
	InputVersion  string      `json:"inputVersion"`
	ObjectVersion string      `json:"objectVersion"`
	OutputVersion string      `json:"outputVersion"`
	Output        ValueOutput `json:"output"`
}

type ValueOutput struct {
	Origin model.ObjectId `json:"origin,omitempty"`
	Value  int            `json:"value"`
}

type TargetState struct {
	ObjectVersion string    `json:"version"`
	Spec          ValueSpec `json:"spec"`
}
