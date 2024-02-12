package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

var ValuePhaseStateAccess = support.NewPhaseStateAccess[*ValueState]()

func init() {
	database.MustRegisterType[ValueState, support.DBObject](Scheme) // Goland requires second type parameter

	// register access to phase info parts in ValueState
	ValuePhaseStateAccess.Register(mymetamodel.PHASE_PROPAGATE, func(o *ValueState) support.PhaseState { return &o.PropagateState })
}

type ValueState struct {
	support.InternalDBObjectSupport `json:",inline"`

	PropagateState `json:",inline"`
}

var _ support.InternalDBObject = (*ValueState)(nil)

func (n *ValueState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(ValuePhaseStateAccess, n))
}

type PropagateState struct {
	support.DefaultPhaseState[ValueCurrentState, ValueTargetState, *ValueCurrentState, *ValueTargetState]
}

type ValueCurrentState struct {
	support.StandardCurrentState
	Owner  string      `json:"owner,omitempty"`
	Output ValueOutput `json:"output"`
}

type ValueOutput struct {
	Origin ObjectId `json:"origin,omitempty"`
	Value  int      `json:"value"`
}

type ValueTargetState struct {
	support.StandardTargetState
	Spec ValueSpec `json:"spec"`
}
