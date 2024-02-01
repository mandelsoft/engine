package valopdemo

import (
	"reflect"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/db"
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

func (n *ValueState) GetCurrentState(phase model.Phase) model.CurrentState {
	return &CurrentState{n}
}

func (n *ValueState) GetTargetState(phase model.Phase) model.TargetState {
	return &TargetState{n}
}

func (n *ValueState) SetExternalState(lcxt common.Logging, ob objectbase.Objectbase, phase model.Phase, state common.ExternalStates) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		t := _o.(*db.ValueState).Target
		if t == nil {
			t = &db.TargetState{}
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
	return err
}

func (n *ValueState) Process(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logging.Logger(db.REALM)

	var out db.ValueResult
	if len(req.Inputs) > 0 {
		links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
		for iid, e := range req.Inputs {
			s := e.(*CurrentCalcState)
			for i, oid := range links {
				if iid == oid {
					out.Value = s.GetOutput()
					out.Origin = iid.ObjectId()
					log.Info("found inbound value from {{link}}: {{value}}", "link", iid, "value", links[i])
					break
				}
			}
		}
	} else {
		out.Value = n.GetTargetState(req.Element.GetPhase()).(*TargetState).GetValue()
		out.Origin = req.Element.Id().ObjectId()
		log.Info("found value from target state: {{value}}", "value", out.Value)
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: db.NewValueResultState(out),
	}
}

type CurrentState struct {
	n *ValueState
}

var _ model.CurrentState = (*CurrentState)(nil)

func (c *CurrentState) get() *db.ValueState {
	return c.n.GetBase().(*db.ValueState)
}

func (c *CurrentState) GetLinks() []model.ElementId {
	var r []model.ElementId

	if c.get().Current.Owner != "" {
		r = append(r, common.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), c.get().Current.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentState) GetInputVersion() string {
	return c.get().Current.InputVersion
}

func (c *CurrentState) GetObjectVersion() string {
	return c.get().Current.ObjectVersion
}

func (c *CurrentState) GetOutputVersion() string {
	return c.get().Current.OutputVersion
}

func (c *CurrentState) GetOutput() int {
	return c.get().Current.Output.Value
}

////////////////////////////////////////////////////////////////////////////////

type TargetState struct {
	n *ValueState
}

var _ model.TargetState = (*TargetState)(nil)

func (c *TargetState) get() *db.TargetState {
	return c.n.GetBase().(*db.ValueState).Target
}

func (c *TargetState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.get()
	if t == nil {
		return nil
	}

	if t.Spec.Owner != "" {
		r = append(r, common.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), t.Spec.Owner, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetState) GetValue() int {
	return c.get().Spec.Value
}
