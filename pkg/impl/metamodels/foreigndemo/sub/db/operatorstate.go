package db

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
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

	// phase specific states
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
	ObservedOperands map[string]string `json:"observedOperands,omitempty"`

	Output GatherOutput `json:"output,omitempty"`
}

type GatherTargetState struct {
	db.StandardTargetState
	Spec OperatorSpec `json:"spec"`
}

type GatherOutput struct {
	Operands   Operands          `json:"operands,omitempty"`
	Operations Operations        `json:"operations,omitempty"`
	Outputs    map[string]string `json:"outputs,omitempty"`
}

type Operands map[string]Operand

func (o Operands) String() string {
	return fmt.Sprintf("%#v", map[string]Operand(o))
}

type Operand struct {
	Origin db.ObjectId `json:"origin,omitempty"`
	Value  int         `json:"value"`
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
	Output ExposeOutput `json:"output,omitempty"`
}

type ExposeTargetState struct {
	db.StandardTargetState
}
