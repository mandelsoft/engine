package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

var ExpressionPhaseStateAccess = support.NewPhaseStateAccess[*ExpressionState]()

func init() {
	database.MustRegisterType[ExpressionState, db.Object](Scheme) // Goland requires second type parameter

	// register access to phase info parts in ExpressionState
	ExpressionPhaseStateAccess.Register(mymetamodel.PHASE_CALCULATE, func(o *ExpressionState) db.PhaseState { return &o.EvaluationState })
}

// ExpressionState handles a foreign controllerd external Expression object
// and is a generated object, also.
// Therefore, it has a dedicated external state type including expression spec and status field
// to reflect the foreign processing state
// and it has an own spec reflecting the provider for generated instances.
type ExpressionState struct {
	db.InternalDBObjectSupport `json:",inline"`

	// Spec is the part of the object state held exclusively in the state object and not
	// on the external object.
	Spec ExpressionStateSpec `json:"spec,omitempty"`

	EvaluationState `json:",inline"`
}

var _ db.InternalDBObject = (*ExpressionState)(nil)

func (n *ExpressionState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(ExpressionPhaseStateAccess, n))
}

type ExpressionStateSpec = db.DefaultSlaveStateSpec

type EvaluationState struct {
	db.DefaultPhaseState[EvaluationCurrentState, EvaluationTargetState, *EvaluationCurrentState, *EvaluationTargetState]
}

type EvaluationCurrentState struct {
	db.StandardCurrentState
	ObservedProvider string `json:"observedProvider,omitempty"`

	Provider string           `json:"provider,omitempty"`
	Output   EvaluationOutput `json:"output"`
}

type EvaluationTargetState struct {
	db.StandardTargetState
	Spec EffectiveExpressionSpec `json:"spec"`
}

type EvaluationOutput = ExpressionOutput

////////////////////////////////////////////////////////////////////////////////

// ExternalExpressionSpec is the formal spec used from the
// Expression object. Because this object is foreign controlled,
// it not only consists of the expressions spec field (as usual),
// but of selected status, also.
// This must be outcome of the foreign processing and information
// requited to detect, whether the actual spec has already been applied.
type ExternalExpressionSpec = db.DefaultForeignControlledExternalObjectSpec[ExpressionSpec, ExpressionOutput]

func NewExternalExpressionSpec(e *Expression) *ExternalExpressionSpec {
	return &ExternalExpressionSpec{
		Spec:            e.Spec,
		Status:          e.Status.Status,
		Message:         e.Status.Message,
		ObservedVersion: e.Status.ObservedVersion,
		Output:          e.Status.Output,
	}
}

////////////////////////////////////////////////////////////////////////////////

// EffectiveExpressionSpec bundles the external spec
// with the internal spec consisting of
// the provider field.
// This field is NOT a spec field for the
// external object and kept for the internal object,
// only, to describe generated expression objects.
type EffectiveExpressionSpec struct {
	*ExternalExpressionSpec `json:",inline"`
	ExpressionStateSpec     `json:",inline"`
}
