package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter
}

type NodeState struct {
	support.DefaultInternalDBObjectSupport `json:",inline"`

	Current CurrentState `json:"current"`
	Target  *TargetState `json:"target,omitempty"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

type CurrentState struct {
	Operands      []string `json:"operands"`
	InputVersion  string   `json:"inputVersion"`
	ObjectVersion string   `json:"objectVersion"`
	OutputVersion string   `json:"outputVersion"`
	Output        Output   `json:"output"`
}

type Output struct {
	Value int `json:"value"`
}

type TargetState struct {
	ObjectVersion string   `json:"version"`
	Spec          NodeSpec `json:"spec"`
}
