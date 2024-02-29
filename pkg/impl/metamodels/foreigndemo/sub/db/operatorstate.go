package db

import (
	"fmt"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

var OperatorPhaseStateAccess = support.NewPhaseStateAccess[*OperatorState]()

func init() {
	database.MustRegisterType[OperatorState, db.Object](Scheme) // Goland requires second type parameter

	// register acc to phase info parts in OperatorState
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_GATHER, func(o *OperatorState) db.PhaseState { return &o.Gather })
	OperatorPhaseStateAccess.Register(mymetamodel.PHASE_EXPOSE, func(o *OperatorState) db.PhaseState { return &o.Expose })
}

type OperatorState struct {
	db.InternalDBObjectSupport `json:",inline"`

	Gather GatherState `json: "gather"`
	Expose ExposeState `json: "expose"`
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
	Operands   Operands          `json:"operands"`
	Operations Operations        `json:"operations"`
	Outputs    map[string]string `json:"outputs"`
}

type Operands map[string]Operand

func (o Operands) String() string {
	return fmt.Sprintf("%#v", map[string]Operand(o))
}

type Operand struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}

func (o Operand) String() string {
	return fmt.Sprintf("%d[%s]", o.Value, o.Origin)
}

////////////////////////////////////////////////////////////////////////////////

type ExposeState struct {
	db.DefaultPhaseState[ExposeCurrentState, ExposeTargetState, *ExposeCurrentState, *ExposeTargetState]
}

type ExposeCurrentState struct {
	db.StandardCurrentState
	Output ExposeOutput `json:"output"`
}

type ExposeTargetState struct {
	db.StandardTargetState
}
