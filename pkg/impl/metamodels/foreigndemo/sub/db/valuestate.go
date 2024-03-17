package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

var ValuePhaseStateAccess = support.NewPhaseStateAccess[*ValueState]()

func init() {
	database.MustRegisterType[ValueState, db.Object](Scheme) // Goland requires second type parameter

	// register access to phase info parts in ValueState
	ValuePhaseStateAccess.Register(mymetamodel.PHASE_PROPAGATE, func(o *ValueState) db.PhaseState { return &o.PropagateState })
}

type ValueState struct {
	db.InternalDBObjectSupport `json:",inline"`

	// Spec is the part of the object state held exclusively in the state object and not
	// on the external object. (there it is found as status)
	Spec ValueStateSpec `json:"spec,omitempty"`

	PropagateState `json:",inline"`
}

var _ db.InternalDBObject = (*ValueState)(nil)

func (n *ValueState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(ValuePhaseStateAccess, n))
}

////////////////////////////////////////////////////////////////////////////////

type ValueStateSpec = db.DefaultSlaveStateSpec

type PropagateState struct {
	db.DefaultPhaseState[ValueCurrentState, ValueTargetState, *ValueCurrentState, *ValueTargetState]
}
type ValueCurrentState struct {
	db.StandardCurrentState
	ObservedProvider string `json:"observedProvider,omitempty"`

	Provider string      `json:"provider,omitempty"`
	Output   ValueOutput `json:"output"`
}

type ValueOutput struct {
	Origin *db.ObjectId `json:"origin,omitempty"`
	Value  int          `json:"value"`
}

type ValueTargetState struct {
	db.StandardTargetState
	Spec EffectiveValueSpec `json:"spec"`
}

// EffectiveValueSpec bundles the external spec
// with the internal spec consisting of
// the provider field.
// This field is NOT a spec field for the
// external object and kept for the internal object,
// only.
type EffectiveValueSpec struct {
	*ValueSpec     `json:",inline"`
	ValueStateSpec `json:",inline"`
}
