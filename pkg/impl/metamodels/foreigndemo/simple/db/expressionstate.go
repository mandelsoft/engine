package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

var ExpressionPhaseStateAccess = support.NewPhaseStateAccess[*ExpressionState]()

func init() {
	database.MustRegisterType[ExpressionState, db.DBObject](Scheme) // Goland requires second type parameter

	// register access to phase info parts in ExpressionState
	ExpressionPhaseStateAccess.Register(mymetamodel.PHASE_CALCULATE, func(o *ExpressionState) db.PhaseState { return &o.EvaluationState })
}

type ExpressionState struct {
	db.InternalDBObjectSupport `json:",inline"`

	EvaluationState `json:",inline"`
}

var _ db.InternalDBObject = (*ExpressionState)(nil)

func (n *ExpressionState) GetStatusValue() string {
	return string(support.CombinedPhaseStatus(ExpressionPhaseStateAccess, n))
}

type EvaluationState struct {
	db.DefaultPhaseState[EvaluationCurrentState, EvaluationTargetState, *EvaluationCurrentState, *EvaluationTargetState]
}

type EvaluationCurrentState struct {
	db.StandardCurrentState

	Output EvaluationOutput `json:"output"`
}

type EvaluationTargetState struct {
	db.StandardTargetState
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
