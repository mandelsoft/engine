package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

var OperatorPhaseStateAccess = support.NewPhaseStateAccess[*OperatorState]()

func init() {
	database.MustRegisterType[OperatorState, support.DBObject](Scheme) // Goland requires second type parameter

	// register acc to phase info parts in OperatorState
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_GATHER, func(o *OperatorState) support.PhaseState { return &o.Gather })
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_CALCULATION, func(o *OperatorState) support.PhaseState { return &o.Calculation })
}

type OperatorState struct {
	support.InternalDBObjectSupport `json:",inline"`

	// phase specific states

	Gather      GatherState      `json: "gather"`
	Calculation CalculationState `json: "calculation"`
}

var _ support.InternalDBObject = (*OperatorState)(nil)

func (n *OperatorState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(OperatorPhaseStateAccess, n))
}

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
	Spec   OperatorSpec `json:"spec"`
	Output GatherOutput `json:"output"`
}

type GatherOutput struct {
	Values []Operand `json:"operands"`
}

type CalculationState struct {
	support.DefaultPhaseState[CalculationCurrentState, CalculationTargetState, *CalculationCurrentState, *CalculationTargetState]
}

type CalculationCurrentState struct {
	support.StandardCurrentState
	Output CalculationOutput `json:"output"`
}

type CalculationTargetState struct {
	support.StandardTargetState
	Operator OperatorName
	Output   CalculationOutput `json:"output"`
}

type CalculationOutput struct {
	Value int `json:"value"`
}

type Operand struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}
