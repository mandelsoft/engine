package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

var NodePhaseStateAccess = support.NewPhaseStateAccess[*NodeState]()

func init() {
	database.MustRegisterType[NodeState, db.DBObject](Scheme) // Goland requires second type parameter

	NodePhaseStateAccess.Register(mymetamodel.PHASE_GATHER, func(o *NodeState) support.PhaseState { return &o.Gather })
	NodePhaseStateAccess.Register(mymetamodel.PHASE_CALCULATION, func(o *NodeState) support.PhaseState { return &o.Calculation })
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`

	// phase specific states

	Gather      GatherState      `json: "gather"`
	Calculation CalculationState `json: "calculation"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

func (n *NodeState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(NodePhaseStateAccess, n))
}

////////////////////////////////////////////////////////////////////////////////

type GatherState struct {
	support.DefaultPhaseState[GatherCurrentState, GatherTargetState, *GatherCurrentState, *GatherTargetState]
}

type GatherCurrentState struct {
	support.StandardCurrentState
	Operands []string     `json:"operands"`
	Output   GatherOutput `json:"output"`
}

type GatherTargetState struct {
	support.StandardTargetState
	Spec NodeSpec `json:"spec"`
}

type GatherOutput struct {
	Values []Operand `json:"operands"`
}

type Operand struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}

////////////////////////////////////////////////////////////////////////////////

type CalculationState struct {
	support.DefaultPhaseState[CalculationCurrentState, CalculationTargetState, *CalculationCurrentState, *CalculationTargetState]
}

type CalculationCurrentState struct {
	support.StandardCurrentState
	Output CalculationOutput `json:"output"`
}

type CalculationTargetState struct {
	support.StandardTargetState
	Operator *OperatorName
}

type CalculationOutput struct {
	Value int `json:"value"`
}
