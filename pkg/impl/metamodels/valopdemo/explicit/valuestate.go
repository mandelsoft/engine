package explicit

import (
	"reflect"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[ValueState](scheme)
}

type ValueState struct {
	support.InternalObjectSupport[*db.ValueState] `json:",inline"`
}

type ValueStateCurrent struct {
	Value int `json:"value"`
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

func (n *ValueState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase Phase, state model.ExternalStates) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o db2.DBObject) (bool, bool) {
		t := n.assureTarget(_o.(*db.ValueState))

		mod := false
		for _, _s := range state { // we have just one external object here, but just for demonstration
			s := _s.(*ExternalValueState).GetState()
			m := !reflect.DeepEqual(t.Spec, *s) || t.GetObjectVersion() != _s.GetVersion()
			if m {
				t.Spec = *s
				t.SetObjectVersion(_s.GetVersion())
			}
			mod = mod || m
		}

		_o.(*db.ValueState).Target = t
		return mod, mod
	})
	return model.ACCEPT_OK, err
}

func (n *ValueState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	var out db.ValueOutput
	if len(req.Inputs) > 0 {
		links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
		for iid, e := range req.Inputs {
			s := e.(*CalcOutputState).GetState()
			for _, oid := range links {
				if iid == oid {
					out.Value = s
					out.Origin = iid.ObjectId()
					log.Info("found inbound value from {{link}}: {{value}}", "link", iid, "value", out.Value)
					break
				}
			}
		}
	} else {
		out.Value = n.GetTargetState(req.Element.GetPhase()).(*TargetValueState).GetValue()
		out.Origin = req.Element.Id().ObjectId()
		log.Info("found value from target state: {{value}}", "value", out.Value)
	}
	return model.StatusCompleted(NewValueOutputState(out))
}

func (n *ValueState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.ValueState](n.commitTargetState))
}

func (n *ValueState) commitTargetState(lctx model.Logging, o *db.ValueState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*ValueOutputState).GetState())
		o.Current.Output.Value = spec.OutputState.(*ValueOutputState).GetState().Value

		log.Info("  owner {{owner}}", "owner", o.Target.Spec.Owner)
		o.Current.Owner = o.Target.Spec.Owner
	}
	o.Target = nil
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

func (c *CurrentValueState) GetLinks() []ElementId {
	var r []ElementId
	t := c.Get()
	if t.Owner != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentValueState) GetOutput() model.OutputState {
	return NewValueOutputState(c.Get().Output)
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

	if t.Spec.Owner != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Spec.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetValueState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetValueState) GetValue() int {
	return c.Get().Spec.Value
}
