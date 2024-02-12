package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

var ExpressionPhaseStateAccess = support.NewPhaseStateAccess[*ExpressionState]()

func init() {
	database.MustRegisterType[ExpressionState, support.DBObject](Scheme) // Goland requires second type parameter

	// register access to phase info parts in ExpressionState
	ExpressionPhaseStateAccess.Register(mymetamodel.PHASE_EVALUATION, func(o *ExpressionState) support.PhaseState { return &o.EvaluationState })
}

type ExpressionState struct {
	support.InternalDBObjectSupport `json:",inline"`

	EvaluationState `json:",inline"`
}

var _ support.InternalDBObject = (*ExpressionState)(nil)

func (n *ExpressionState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(ExpressionPhaseStateAccess, n))
}

type EvaluationState struct {
	support.DefaultPhaseState[EvaluationCurrentState, EvaluationTargetState, *EvaluationCurrentState, *EvaluationTargetState]
}

type EvaluationCurrentState struct {
	support.StandardCurrentState

	Output EvaluationOutput `json:"output"`
}

type EvaluationTargetState struct {
	support.StandardTargetState
	Spec EffectiveExpressionSpec `json:"spec"`
}

type EvaluationOutput = ExpressionOutput

////////////////////////////////////////////////////////////////////////////////

type EffectiveExpressionSpec struct {
	Spec            ExpressionSpec   `json:"spec"`
	Status          model.Status     `json:"status"`
	Message         string           `json:"message"`
	ObservedVersion string           `json:"observervedVersion"`
	Output          ExpressionOutput `json:"output"`
}

func NewEffectiveExpressionSpec(e *Expression) *EffectiveExpressionSpec {
	return &EffectiveExpressionSpec{
		Spec:            e.Spec,
		Status:          e.Status.Status,
		Message:         e.Status.Message,
		ObservedVersion: e.Status.ObservedVersion,
		Output:          e.Status.Output,
	}
}

func (e *EffectiveExpressionSpec) GetSpecVersion() string {
	return support.NewState(&e.Spec).GetVersion()
}

func (e *EffectiveExpressionSpec) IsDone() bool {
	return support.NewState(&e.Spec).GetVersion() == e.ObservedVersion
}
