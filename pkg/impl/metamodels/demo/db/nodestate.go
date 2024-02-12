package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

var NodePhaseStateAccess = support.NewPhaseStateAccess[*NodeState]()

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter

	NodePhaseStateAccess.Register(mymetamodel.PHASE_UPDATING, func(o *NodeState) support.PhaseState { return &o.State })
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`

	State State `json:"state"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

func (n *NodeState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(NodePhaseStateAccess, n))
}

type State = support.DefaultPhaseState[CurrentState, TargetState, *CurrentState, *TargetState]

type CurrentState struct {
	support.StandardCurrentState
	Operands []string `json:"operands"`
	Output   Output   `json:"output"`
}

type Output struct {
	Value int `json:"value"`
}

type TargetState struct {
	support.StandardTargetState
	Spec NodeSpec `json:"spec"`
}
