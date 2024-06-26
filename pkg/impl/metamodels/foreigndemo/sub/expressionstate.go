package sub

import (
	"errors"
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/generics"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	wrapped.MustRegisterType[ExpressionState](scheme)
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

func (n *ExpressionState) GetExternalState(o model.ExternalObject, phase Phase) model.ExternalState {
	// incorporate actual binding into state
	return n.EffectiveTargetSpec(o.GetState())
}

func (n *ExpressionState) assureTarget(o *db.ExpressionState) *db.EvaluationTargetState {
	return o.CreateTarget().(*db.EvaluationTargetState)
}

func (n *ExpressionState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, state model.ExternalState) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		t := n.assureTarget(_o.(*db.ExpressionState))
		log := lctx.Logger(REALM)

		mod := false
		if state == nil {
			// external object not existent
			state = n.EffectiveTargetSpec(nil)
		}
		s := state.(*EffectiveExpressionState).GetState()
		if s == nil {
			s = &db.EffectiveExpressionSpec{}
		}
		log.Info("accepting object version {{version}} provider {{provider}}", "version", state.GetVersion(), "provider", s.Extension.Provider)
		s.ApplyFormalObjectVersion(log, t, &mod)
		support.UpdateField(&t.Spec, s, &mod)
		support.UpdateField(&t.ObjectVersion, generics.Pointer(state.GetVersion()), &mod)
		return mod, mod
	})
	return 0, err
}

func (n *ExpressionState) EffectiveTargetSpec(state model.ExternalState) *EffectiveExpressionState {
	var v *db.ExternalExpressionSpec
	if state != nil {
		v = state.(*ExternalExpressionState).GetState()
	}
	return NewEffectiveExpressionState(
		db2.NewDefaultEffectiveSlaveObjectSpec(v, &n.GetBase().(*db.ExpressionState).Spec),
	)
}

func (n *ExpressionState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	if req.Delete {
		return support.HandleExternalObjectDeletionRequest(log, req.Model.ObjectBase(), mymetamodel.TYPE_EXPRESSION, req.Element.Id())
	}

	target := NewTargetEvaluationState(n).Get()

	var ex *db.ExpressionSpec

	if target.Spec.Extension.Provider == "" {
		log.Info("expression has no provider -> no update")
		ex = &n.GetTargetState(mymetamodel.PHASE_CALCULATE).(*TargetEvaluationState).GetSpec().External.Spec
		log.Info("using spec from external object: {{spec}}", "spec", general.DescribeObject(ex))
	} else {
		var gathered *db.GatherOutput
		for iid, e := range req.Inputs {
			gathered = e.(*GatherOutputState).GetState()
			log.Info("found output from {{link}}", "link", iid)
		}

		ex = db.NewExpressionSpec()
		for n, v := range gathered.Operands {
			log.Info("- using operand {{name}}({{value}}) from {{oid}}", "name", n, "value", v.Value, "oid", v.Origin)
			ex.AddOperand(n, v.Value)
		}
		for n, o := range gathered.Operations {
			log.Info("- using operation {{name}}({{value}})", "name", n, "value", o)
			if o.Operator == db.OP_EXPR {
				ex.AddExpressionOperation(n, o.Expression)

			} else {
				ex.AddOperation(n, o.Operator, o.Operands...)
			}
		}

		updated, _, err := req.SlaveManagement.AssureExternal(
			support.ExternalUpdateFunc(func(o *db.Expression) bool {
				mod := false
				if support.UpdateField(&o.Spec, ex, &mod) {
					log.Info("- update spec {{spec}}", "spec", ex)
				}

				if support.UpdateField(&o.Status.Provider, generics.Pointer(target.Spec.Extension.Provider), &mod) {
					log.Info("- update provider {{provider}}", "provider", target.Spec.Extension.Provider)
				}

				return mod
			}),
			database.NewObjectId(mymetamodel.TYPE_EXPRESSION, n.GetNamespace(), n.GetName()),
		)
		if err != nil {
			return model.StatusCompleted(nil, err)
		}
		if updated {
			log.Info("expression updated -> wait for next status change")
			return model.StatusWaiting()
		}
	}

	log.Info("found expression version {{version}} ", "version", target.Spec.External.ObservedVersion)
	required := ex.GetVersion()
	if required != target.Spec.External.ObservedVersion {
		log.Info("required version {{required}} not reached -> wait for next change", "required", required)
		return model.StatusWaiting()
	}
	log.Info("required version {{required}} reached -> propagate expression results", "required", required)

	if target.Spec.External.Status != model.STATUS_COMPLETED {
		log.Warn("expression processing failed with status {{status}}[{{message}}]", "status", target.Spec.External.Status, "message", target.Spec.External.Message)
		return model.StatusFailed(fmt.Errorf("expression processing failed with status %q[%s]", target.Spec.External.Status, target.Spec.External.Message))
	}

	return model.StatusCompleted(NewEvaluationOutputState(req.FormalVersion, target.Spec.External.Output))
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
		updated, err = wrapped.Modify(ob, o, func(_o db2.Object) (bool, bool) {
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

func (n *ExpressionState) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, tgt model.TargetState, formal *string) (bool, error) {
	return n.InternalObjectSupport.HandleRollback(lctx, ob, phase, id, tgt, formal, support.RollbackFunc[*db.ExpressionState](n.rollbackTargetState))
}

func (n *ExpressionState) rollbackTargetState(lctx model.Logging, o *db.ExpressionState, phase Phase) {
}

func (n *ExpressionState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.ExpressionState](n.commitTargetState))
}

func (n *ExpressionState) commitTargetState(lctx model.Logging, o *db.ExpressionState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		log.Info("  output {{output}}", "output", general.DescribeObject(spec.OutputState.(*EvaluationOutputState).GetState()))
		o.Current.Output = spec.OutputState.(*EvaluationOutputState).GetState()
		log.Info("  provider {{provider}}", "output", o.Target.Spec.Extension.Provider)
		o.Current.Provider = o.Target.Spec.Extension.Provider

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
	return &CurrentEvaluationState{support.NewCurrentStateSupport[*db.ExpressionState, *db.EvaluationCurrentState](n, mymetamodel.PHASE_CALCULATE)}
}

func (s *CurrentEvaluationState) GetObservedState() model.ObservedState {
	if s.GetObservedVersion() == s.GetObjectVersion() {
		return s
	}
	return s.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_OPERATOR_STATE, mymetamodel.PHASE_GATHER, s.Get().ObservedProvider)
}

func (c *CurrentEvaluationState) GetLinks() []ElementId {

	var r []ElementId

	if c.Get().Provider != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), c.Get().Provider, mymetamodel.PHASE_GATHER))
	}
	return r
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
	return &TargetEvaluationState{support.NewTargetStateSupport[*db.ExpressionState, *db.EvaluationTargetState](n, mymetamodel.PHASE_CALCULATE)}
}

func (c *TargetEvaluationState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	if t.Spec.Extension.Provider != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Spec.Extension.Provider, mymetamodel.PHASE_GATHER))
	}
	return r
}

func (c *TargetEvaluationState) GetProvider() string {
	return c.Get().Spec.Extension.Provider
}

func (c *TargetEvaluationState) GetSpec() *db.EffectiveExpressionSpec {
	return &c.Get().Spec
}
