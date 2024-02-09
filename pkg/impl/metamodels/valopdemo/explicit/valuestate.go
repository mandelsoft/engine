package explicit

import (
	"reflect"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[ValueState](scheme)
}

type ValueState struct {
	support.InternalObjectSupport
}

type ValueStateCurrent struct {
	Value int `json:"value"`
}

var _ model.InternalObject = (*ValueState)(nil)

func (n *ValueState) GetCurrentState(phase Phase) model.CurrentState {
	return &CurrentValueState{n}
}

func (n *ValueState) GetTargetState(phase Phase) model.TargetState {
	return &TargetValueState{n}
}

func (n *ValueState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, state model.ExternalStates) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		t := _o.(*db.ValueState).Target
		if t == nil {
			t = &db.ValueTargetState{}
		}

		mod := false
		for _, _s := range state { // we have just one external object here, but just for demonstration
			s := _s.(*ExternalValueState).GetState()
			m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != _s.GetVersion()
			if m {
				t.Spec = *s
				t.ObjectVersion = _s.GetVersion()
			}
			mod = mod || m
		}

		_o.(*db.ValueState).Target = t
		return mod, mod
	})
	return model.ACCEPT_OK, err
}

func (n *ValueState) Process(req model.Request) model.ProcessingREsult {
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
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, n.GetTargetState(phase).GetObjectVersion(), support.CommitFunc(n.commitTargetState))
}

func (n *ValueState) commitTargetState(lctx model.Logging, _o support.InternalDBObject, phase Phase, spec *model.CommitInfo) {
	o := _o.(*db.ValueState)
	log := lctx.Logger(REALM)
	if nil != o.Target && spec != nil {
		o.Current.InputVersion = spec.InputVersion
		log.Info("Commit object version for ValueState {{name}}", "name", o.Name)
		log.Info("  object version {{version}}", "version", o.Target.ObjectVersion)
		o.Current.ObjectVersion = o.Target.ObjectVersion
		o.Current.OutputVersion = spec.State.(*ValueOutputState).GetOutputVersion()
		o.Current.Output.Value = spec.State.(*ValueOutputState).GetState().Value

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
	n *ValueState
}

var _ model.CurrentState = (*CurrentValueState)(nil)

func (c *CurrentValueState) get() *db.ValueState {
	return c.n.GetBase().(*db.ValueState)
}

func (c *CurrentValueState) GetLinks() []ElementId {
	var r []ElementId

	if c.get().Current.Owner != "" {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), c.get().Current.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentValueState) GetObservedVersion() string {
	return c.n.GetDBObject().GetObservedVersion(mymetamodel.PHASE_PROPAGATE)
}

func (c *CurrentValueState) GetInputVersion() string {
	return c.get().Current.InputVersion
}

func (c *CurrentValueState) GetObjectVersion() string {
	return c.get().Current.ObjectVersion
}

func (c *CurrentValueState) GetOutputVersion() string {
	return c.get().Current.OutputVersion
}

func (c *CurrentValueState) GetOutput() model.OutputState {
	return NewValueOutputState(c.get().Current.Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetValueState struct {
	n *ValueState
}

var _ model.TargetState = (*TargetValueState)(nil)

func (c *TargetValueState) get() *db.ValueTargetState {
	return c.n.GetBase().(*db.ValueState).Target
}

func (c *TargetValueState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.get()
	if t == nil {
		return nil
	}

	if t.Spec.Owner != "" {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), t.Spec.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetValueState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetValueState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetValueState) GetValue() int {
	return c.get().Spec.Value
}
