package db

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
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

type GatherCurrentState struct {
	db.StandardCurrentState
	Output GatherOutput `json:"output"`
}

type GatherTargetState struct {
	db.StandardTargetState
	Spec OperatorSpec `json:"spec"`
}

type GatherOutput struct {
	Operands   OperandInfos `json:"operands"`
	Operations Operations   `json:"operations"`
}

type OperandInfos map[string]OperandInfo

func (o OperandInfos) String() string {
	return fmt.Sprintf("%#v", map[string]OperandInfo(o))
}

type OperandInfo struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}

func (o OperandInfo) String() string {
	return fmt.Sprintf("%d[%s]", o.Value, o.Origin)
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
	Operands map[string]string `json:"operands,omitempty"`
	Outputs  map[string]string `json:"outputs,omitempty"`
}