package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`

	// shared state for all phases.
	// This stores the node state commonly fixed for all phases when the first phase is started.

	Current ObjectCurrentState `json:"current"`
	Target  *ObjectTargetState `json:"target,omitempty"`

	// phase specif states

	Gather struct {
		Current GatherCurrentState `json:"current"`
		Target  *GatherTargetState `json:"target,omitempty"`
	} `json: "gather"`
	Calculation struct {
		Current CalculationCurrentState `json:"current"`
		Target  *CalculationTargetState `json:"target,omitempty"`
	} `json: "calculation"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

type ObjectCurrentState struct {
	Operands []string `json:"operands"`
}

type ObjectTargetState struct {
	Spec          NodeSpec `json:"spec"`
	ObjectVersion string   `json:"objectVersion"`
}

type GatherCurrentState struct {
	InputVersion  string       `json:"inputVersion"`
	ObjectVersion string       `json:"objectVersion"`
	OutputVersion string       `json:"outputVersion"`
	Output        GatherOutput `json:"output"`
}

type CalculationCurrentState struct {
	InputVersion  string            `json:"inputVersion"`
	ObjectVersion string            `json:"objectVersion"`
	OutputVersion string            `json:"outputVersion"`
	Output        CalculationOutput `json:"output"`
}

type GatherOutput struct {
	Values []Operand `json:"operands"`
}

type CalculationOutput struct {
	Value int `json:"value"`
}

type GatherTargetState struct {
	ObjectVersion string `json:"version"`
}

type CalculationTargetState struct {
	ObjectVersion string `json:"version"`
}

type Operand struct {
	Origin model.ObjectId `json:"origin,omitempty"`
	Value  int            `json:"value"`
}
