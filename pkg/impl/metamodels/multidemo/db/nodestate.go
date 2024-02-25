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

	NodePhaseStateAccess.Register(mymetamodel.PHASE_GATHER, func(o *NodeState) db.PhaseState { return &o.Gather })
	NodePhaseStateAccess.Register(mymetamodel.PHASE_CALCULATION, func(o *NodeState) db.PhaseState { return &o.Calculation })
}

type NodeState struct {
	db.InternalDBObjectSupport `json:",inline"`

	// phase specific states

	Gather      GatherState      `json: "gather"`
	Calculation CalculationState `json: "calculation"`
}

var _ db.InternalDBObject = (*NodeState)(nil)

func (n *NodeState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(NodePhaseStateAccess, n))
}

////////////////////////////////////////////////////////////////////////////////

type GatherState struct {
	db.DefaultPhaseState[GatherCurrentState, GatherTargetState, *GatherCurrentState, *GatherTargetState]
}

type GatherCurrentState struct {
	db.StandardCurrentState
	Operands []string     `json:"operands"`
	Output   GatherOutput `json:"output"`
}

type GatherTargetState struct {
	db.StandardTargetState
	Spec NodeSpec `json:"spec"`
}

type GatherOutput struct {
	Operator *OperatorName `json:"operator,omitempty"`
	Operands []Operand     `json:"operands"`
}

type Operand struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}

////////////////////////////////////////////////////////////////////////////////

type CalculationState struct {
	db.DefaultPhaseState[CalculationCurrentState, CalculationTargetState, *CalculationCurrentState, *CalculationTargetState]
}

type CalculationCurrentState struct {
	db.StandardCurrentState
	Output CalculationOutput `json:"output"`
}

type CalculationTargetState struct {
	db.StandardTargetState
}

type CalculationOutput struct {
	Value int `json:"value"`
}
