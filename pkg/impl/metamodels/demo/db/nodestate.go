package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

var NodePhaseStateAccess = support.NewPhaseStateAccess[*NodeState]()

func init() {
	database.MustRegisterType[NodeState, db.Object](Scheme) // Goland requires second type parameter

	NodePhaseStateAccess.Register(mymetamodel.PHASE_UPDATING, func(o *NodeState) db.PhaseState { return &o.State })
}

type NodeState struct {
	db.InternalDBObjectSupport `json:",inline"`

	State State `json:"state"`
}

var _ db.InternalDBObject = (*NodeState)(nil)

func (n *NodeState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(NodePhaseStateAccess, n))
}

type State = db.DefaultPhaseState[CurrentState, TargetState, *CurrentState, *TargetState]

type CurrentState struct {
	db.StandardCurrentState
	Operands []string `json:"operands"`
	Output   Output   `json:"output"`
}

type Output struct {
	Value int `json:"value"`
}

type TargetState struct {
	db.StandardTargetState
	Spec NodeSpec `json:"spec"`
}
