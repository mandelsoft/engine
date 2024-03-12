package explicit

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[ValueState](scheme)
}

type ValueState struct {
	support.InternalObjectSupport[*db.ValueState] `json:",inline"`
}

var _ model.InternalObject = (*ValueState)(nil)

func (n *ValueState) Initialize() error {
	return support.SetPhaseStateAccess(n, db.ValuePhaseStateAccess)
}

func (n *ValueState) GetCurrentState(phase Phase) model.CurrentState {
	return NewCurrentValueState(n)
}

func (n *ValueState) GetTargetState(phase Phase) model.TargetState {
	return NewTargetValueState(n)
}

func (n *ValueState) assureTarget(o *db.ValueState) *db.ValueTargetState {
	return o.CreateTarget().(*db.ValueTargetState)
}

func (n *ValueState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase Phase, state model.ExternalState) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		t := n.assureTarget(_o.(*db.ValueState))

		mod := false
		if state == nil {
			mod = true
		} else {
			s := state.(*ExternalValueState).GetState()
			support.UpdateField(&t.Spec, s, &mod)
			support.UpdateField(&t.ObjectVersion, utils.Pointer(state.GetVersion()), &mod)
		}
		return mod, mod
	})
	return model.ACCEPT_OK, err
}

func (n *ValueState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)
	target := n.GetTargetState(req.Element.GetPhase())

	if req.Delete {
		return support.HandleExternalObjectDeletionRequest(log, req.Model.ObjectBase(), mymetamodel.TYPE_VALUE, req.Element.Id())
	}

	var out db.ValueOutput
	if len(req.Inputs) > 0 {
		links := target.GetLinks()
		for iid, e := range req.Inputs {
			s := e.(*CalcOutputState).GetState()
			for _, oid := range links {
				if iid == oid {
					out.Value = s
					out.Origin = db2.NewObjectIdFor(iid)
					log.Info("found inbound value from {{link}}: {{value}}", "link", iid, "value", out.Value)
				}
				break
			}
		}
	} else {
		out.Value = n.GetTargetState(req.Element.GetPhase()).(*TargetValueState).GetValue()
		out.Origin = db2.NewObjectIdFor(req.Element)
		log.Info("found value from target state: {{value}}", "value", out.Value)
	}
	return model.StatusCompleted(NewValueOutputState(req.FormalVersion, out))
}

func (n *ValueState) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, tgt model.TargetState, formal *string) (bool, error) {
	return n.InternalObjectSupport.HandleRollback(lctx, ob, phase, id, tgt, formal, support.RollbackFunc[*db.ValueState](n.rollbackTargetState))
}

func (n *ValueState) rollbackTargetState(lctx model.Logging, o *db.ValueState, phase Phase) {
	log := lctx.Logger(REALM)
	if o.Target != nil {
		log.Info("  observed provider {{provider}}", "provider", o.Target.Spec.Provider)
		o.Current.ObservedProvider = o.Target.Spec.Provider
	}
}

func (n *ValueState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.ValueState](n.commitTargetState))
}

func (n *ValueState) commitTargetState(lctx model.Logging, o *db.ValueState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*ValueOutputState).GetState())
		o.Current.Output.Value = spec.OutputState.(*ValueOutputState).GetState().Value

		log.Info("  provider {{provider}}", "provider", o.Target.Spec.Provider)
		o.Current.Provider = o.Target.Spec.Provider
		o.Current.ObservedProvider = o.Target.Spec.Provider
	} else {
		log.Info("nothing to commit for phase {{phase}} of ValueState {{name}}")
	}
}

////////////////////////////////////////////////////////////////////////////////

type ValueOutputState = support.OutputState[db.ValueOutput]

var NewValueOutputState = support.NewOutputState[db.ValueOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentValueState struct {
	support.CurrentStateSupport[*db.ValueState, *db.ValueCurrentState]
}

var _ model.CurrentState = (*CurrentValueState)(nil)

func NewCurrentValueState(n *ValueState) model.CurrentState {
	return &CurrentValueState{support.NewCurrentStateSupport[*db.ValueState, *db.ValueCurrentState](n, mymetamodel.PHASE_PROPAGATE)}
}

func (s *CurrentValueState) GetObservedState() model.ObservedState {
	if s.GetObservedVersion() == s.GetObjectVersion() {
		return s
	}
	return s.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_OPERATOR_STATE, mymetamodel.PHASE_CALCULATION, s.Get().ObservedProvider)
}

func (c *CurrentValueState) GetLinks() []ElementId {
	var r []ElementId

	t := c.Get()
	if t.Provider != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Provider, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentValueState) GetProvider() string {
	return c.Get().Provider
}

func (c *CurrentValueState) GetOutput() model.OutputState {
	return NewValueOutputState(c.GetFormalVersion(), c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetValueState struct {
	support.TargetStateSupport[*db.ValueState, *db.ValueTargetState]
}

var _ model.TargetState = (*TargetValueState)(nil)

func NewTargetValueState(n *ValueState) model.TargetState {
	return &TargetValueState{support.NewTargetStateSupport[*db.ValueState, *db.ValueTargetState](n, mymetamodel.PHASE_PROPAGATE)}
}

func (c *TargetValueState) GetLinks() []ElementId {
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	if t.Spec.Provider != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Spec.Provider, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetValueState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetValueState) GetProvider() string {
	return c.Get().Spec.Provider
}

func (c *TargetValueState) GetValue() int {
	return c.Get().Spec.Value
}
