package sub

import (
	"errors"
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	wrapped2 "github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	wrapped2.MustRegisterType[ExpressionState](scheme)
}

type ExpressionState struct {
	support.InternalObjectSupport[*db.ExpressionState] `json:",inline"`
}

var _ model.InternalObject = (*ExpressionState)(nil)

func (n *ExpressionState) Initialize() error {
	return support.SetPhaseStateAccess(n, db.ExpressionPhaseStateAccess)
}

func (n *ExpressionState) GetCurrentState(phase Phase) model.CurrentState {
	return NewCurrentExpressionState(n)
}

func (n *ExpressionState) GetTargetState(phase Phase) model.TargetState {
	return NewTargetEvaluationState(n)
}

func (n *ExpressionState) assureTarget(o *db.ExpressionState) *db.EvaluationTargetState {
	return o.CreateTarget().(*db.EvaluationTargetState)
}

func (n *ExpressionState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, state model.ExternalStates) (model.AcceptStatus, error) {
	_, err := wrapped2.Modify(ob, n, func(_o db2.DBObject) (bool, bool) {
		t := n.assureTarget(_o.(*db.ExpressionState))

		mod := false
		if len(state) == 0 {
			// external object not existent
			state = model.ExternalStates{"": NewExternalExpressionState(nil)}
		}
		for _, _s := range state { // we have just one external object here, but just for demonstration
			s := _s.(*ExternalExpressionState).GetState()
			if s == nil {
				s = &db.EffectiveExpressionSpec{}
			}
			support.UpdateField(&t.Spec, s, &mod)
			support.UpdateField(&t.ObjectVersion, utils.Pointer(_s.GetVersion()), &mod)
		}
		return mod, mod
	})
	return 0, err
}

func (n *ExpressionState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	if req.Delete {
		log.Info("deleting successful")
		return model.StatusDeleted()
	}

	target := NewTargetEvaluationState(n).Get()

	var gathered *db.GatherOutput
	for iid, e := range req.Inputs {
		gathered = e.(*GatherOutputState).GetState()
		log.Info("found output from {{link}}", "link", iid)
	}

	ex := db.NewExpressionSpec()
	for n, v := range gathered.Operands {
		log.Info("- using operand {{name}}({{value}}) from {{oid}}", "name", n, "value", v.Value, v.Origin)
		ex.AddOperand(n, v.Value)
	}
	for n, o := range gathered.Operations {
		log.Info("- using operation {{name}}({{value}})", "name", n, "value", o)
		ex.AddOperation(n, o.Operator, o.Operands...)
	}

	updated, err := n.assureSlave(log, req.Model.ObjectBase(), ex)
	if err != nil {
		return model.StatusCompleted(nil, err)
	}
	if updated {
		log.Info("expression updated -> wait for next status change")
		return model.StatusWaiting()
	}

	log.Info("found expression version {{version}} ", "version", target.Spec.ObservedVersion)
	required := ex.GetVersion()
	if required != target.Spec.ObservedVersion {
		log.Info("required version {{required}} not reached -> wait for next change", "required", required)
		return model.StatusWaiting()
	}

	if target.Spec.Status != model.STATUS_COMPLETED {
		log.Warn("expression processing failed with status {{status}}[{{message}}]", "status", target.Spec.Status, "message", target.Spec.Message)
		return model.StatusFailed(fmt.Errorf("expression processing failed with status %q[%s]", target.Spec.Status, target.Spec.Message))
	}

	return model.StatusCompleted(NewEvaluationOutputState(req.FormalVersion, target.Spec.Output))
}

func (n *ExpressionState) assureSlave(log logging.Logger, ob objectbase.Objectbase, ex *db.ExpressionSpec) (bool, error) {
	extid := database.NewObjectId(mymetamodel.TYPE_EXPRESSION, n.GetNamespace(), n.GetName())
	log = log.WithValues("extid", extid)
	log.Info("checking slave expression object {{extid}}")

	mode := "update"
	updated := false

	_o, err := ob.GetObject(extid)
	if errors.Is(err, database.ErrNotExist) {
		log.Info("expression object {{extid}} not found")
		_o, err = ob.CreateObject(extid)
		if err != nil {
			log.LogError(err, "creation of expression object {{exitid}} failed")
			return false, err
		}
		mode = "create"
	}

	if err == nil {
		o := _o.(*Expression)
		updated, err = wrapped2.Modify(ob, o, func(_o db2.DBObject) (bool, bool) {
			o := _o.(*db.Expression)
			mod := false
			support.UpdateField(&o.Spec, ex, &mod)
			return mod, mod
		})
	}
	if err != nil {
		log.LogError(err, fmt.Sprintf("%s of expression object {{exitid}} failed", mode))
		return false, err
	}
	if updated {
		log.Info(fmt.Sprintf("slave expression object {{extid}} %s", mode+"d"))
	} else {
		log.Info("slave expression object spec for {{extid}} up to date", "extid", extid)
	}
	return updated, nil
}

func (n *ExpressionState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.ExpressionState](n.commitTargetState))
}

func (n *ExpressionState) commitTargetState(lctx model.Logging, o *db.ExpressionState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*EvaluationOutputState).GetState())
		o.Current.Output = spec.OutputState.(*EvaluationOutputState).GetState()
	} else {
		log.Info("nothing to commit for phase {{phase}} of ExpressionState {{name}}")
	}
}

////////////////////////////////////////////////////////////////////////////////

type EvaluationOutputState = support.OutputState[db.EvaluationOutput]

var NewEvaluationOutputState = support.NewOutputState[db.EvaluationOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentEvaluationState struct {
	support.CurrentStateSupport[*db.ExpressionState, *db.EvaluationCurrentState]
}

var _ model.CurrentState = (*CurrentEvaluationState)(nil)

func NewCurrentExpressionState(n *ExpressionState) model.CurrentState {
	return &CurrentEvaluationState{support.NewCurrentStateSupport[*db.ExpressionState, *db.EvaluationCurrentState](n, mymetamodel.PHASE_EVALUATION)}
}

func (c *CurrentEvaluationState) GetLinks() []ElementId {
	return []ElementId{NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), c.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *CurrentEvaluationState) GetOutput() model.OutputState {
	return NewEvaluationOutputState(c.GetFormalVersion(), c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetEvaluationState struct {
	support.TargetStateSupport[*db.ExpressionState, *db.EvaluationTargetState]
}

var _ model.TargetState = (*TargetEvaluationState)(nil)

func NewTargetEvaluationState(n *ExpressionState) *TargetEvaluationState {
	return &TargetEvaluationState{support.NewTargetStateSupport[*db.ExpressionState, *db.EvaluationTargetState](n, mymetamodel.PHASE_EVALUATION)}
}

func (c *TargetEvaluationState) GetLinks() []mmids.ElementId {
	return []ElementId{NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), c.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *TargetEvaluationState) GetSpec() *db.EffectiveExpressionSpec {
	return &c.Get().Spec
}
