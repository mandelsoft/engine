package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

var OperatorPhaseStateAccess = support.NewPhaseStateAccess[*OperatorState]()

func init() {
	database.MustRegisterType[OperatorState, db.DBObject](Scheme) // Goland requires second type parameter

	// register acc to phase info parts in OperatorState
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_GATHER, func(o *OperatorState) db.PhaseState { return &o.Gather })
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_CALCULATION, func(o *OperatorState) db.PhaseState { return &o.Calculation })
}

type OperatorState struct {
	db.InternalDBObjectSupport `json:",inline"`

	Gather      GatherState      `json: "gather"`
	Calculation CalculationState `json: "calculation"`
}

var _ db.InternalDBObject = (*OperatorState)(nil)

func (n *OperatorState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(OperatorPhaseStateAccess, n))
}

////////////////////////////////////////////////////////////////////////////////

type GatherState struct {
	db.DefaultPhaseState[GatherCurrentState, GatherTargetState, *GatherCurrentState, *GatherTargetState]
}

type ObjectTargetState struct {
	Spec          OperatorSpec `json:"spec"`
	ObjectVersion string       `json:"objectVersion"`
}

type GatherCurrentState struct {
	db.StandardCurrentState
	Operands []string     `json:"operands,omitempty"`
	Output   GatherOutput `json:"output"`
}

type GatherTargetState struct {
	db.StandardTargetState
	Spec OperatorSpec `json:"spec"`
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
	db.DefaultPhaseState[CalculationCurrentState, CalculationTargetState, *CalculationCurrentState, *CalculationTargetState]
}

type CalculationCurrentState struct {
	db.StandardCurrentState
	Output CalculationOutput `json:"output"`
}

type CalculationTargetState struct {
	db.StandardCurrentState
	Operations []Operation `json:"operations,omitempty"`
}
