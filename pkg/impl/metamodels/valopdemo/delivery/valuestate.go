package delivery

import (
	"errors"
	"reflect"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
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
	return &CurrentValueState{n}
}

func (n *ValueState) GetTargetState(phase model.Phase) model.TargetState {
	return &TargetValueState{n}
}

func (n *ValueState) SetExternalState(lcxt common.Logging, ob objectbase.Objectbase, phase model.Phase, state common.ExternalStates) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		t := _o.(*db.ValueState).Target
		if t == nil {
			t = &db.ValueTargetState{}
		}

		mod := false
		if len(state) == 0 {
			// external object not existent
			state = common.ExternalStates{"": nil}
		}
		for _, _s := range state { // we have just one external object here, but just for demonstration
			_s = n.EffectiveTargetSpec(_s) // incorporate local state
			s := _s.(*EffectiveValueState).GetState()

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

func (n *ValueState) EffectiveTargetSpec(state model.ExternalState) *EffectiveValueState {
	var v *db.ValueSpec
	if state != nil {
		v = state.(*ExternalValueState).GetState()
	}
	return NewEffectiveValueState(
		&db.EffectiveValueSpec{
			ValueSpec:      v,
			ValueStateSpec: n.GetBase().(*db.ValueState).Spec,
		})
}

func (n *ValueState) Process(req common.Request) common.Status {
	log := req.Logging.Logger(REALM)

	target := n.GetTargetState(req.Element.GetPhase())

	var out db.ValueOutput
	if len(req.Inputs) > 0 {
		links := target.GetLinks()
		for iid, e := range req.Inputs {
			s := e.(*CalcOutputState).GetState()
			for _, oid := range links {
				if iid == oid {
					if v, ok := s[n.GetName()]; ok {
						out.Value = v
						out.Origin = utils.Pointer(iid.ObjectId())
						log.Info("found inbound value from {{link}}: {{value}}", "link", iid, "value", out.Value)
					}
					break
				}
			}
		}
	} else {
		out.Value = n.GetTargetState(req.Element.GetPhase()).(*TargetValueState).GetValue()
		out.Origin = utils.Pointer(req.Element.Id().ObjectId())
		log.Info("found value from target state: {{value}}", "value", out.Value)
	}

	if out.Origin != nil {
		err := n.assureSlave(log, req.Model.ObjectBase(), &out)
		if err != nil {
			return model.Status{
				Status: common.STATUS_COMPLETED,
				Error:  err,
			}
		}
		return model.Status{
			Status:      common.STATUS_COMPLETED,
			ResultState: NewValueOutputState(out),
		}
	}

	log.Info("provider {{provider}} does not feed value anymore", "provider", target.(*TargetValueState).GetProvider())
	log.Info("deleting slave value object")

	err := req.Model.ObjectBase().DeleteObject(database.NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName()))
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return model.Status{
				Status: common.STATUS_COMPLETED,
				Error:  err,
			}
		}
	}
	log.Info("deleting value state object")
	err = req.Model.ObjectBase().DeleteObject(n)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return model.Status{
				Status: common.STATUS_COMPLETED,
				Error:  err,
			}
		}
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		Deleted:     true,
		ResultState: nil,
	}
}

func (n *ValueState) assureSlave(log logging.Logger, ob common.Objectbase, out *db.ValueOutput) error {
	extid := database.NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName())
	log = log.WithValues("extid", extid)
	if *out.Origin != common.NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName()) {
		log.Info("checking slave value object {{extid}}")
		_, err := ob.GetObject(extid)
		if errors.Is(err, database.ErrNotExist) {
			log.Info("value object {{extid}} not found")
			_o, err := ob.CreateObject(extid)
			o := _o.(*Value)
			if err == nil {
				_, err = wrapped.Modify(ob, o, func(_o support.DBObject) (bool, bool) {
					o := _o.(*db.Value)
					mod := false
					support.UpdateField(&o.Spec.Value, &out.Value, &mod)
					support.UpdateField(&o.Status.Provider, utils.Pointer(out.Origin.GetName()), &mod)
					return mod, mod
				},
				)
			}
			if err != nil {
				log.LogError(err, "creation of value object {{exitid}} failed")
				return err
			}
			log.Info("slave value object {{extid}} created")
		}
	}
	return nil
}

func (n *ValueState) Commit(lctx common.Logging, ob objectbase.Objectbase, phase common.Phase, id model.RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.Commit(lctx, ob, phase, id, commit, support.CommitFunc(n.commitTargetState))
}

func (n *ValueState) commitTargetState(lctx common.Logging, _o support.InternalDBObject, phase model.Phase, spec *model.CommitInfo) {
	o := _o.(*db.ValueState)
	log := lctx.Logger(REALM)
	if nil != o.Target && spec != nil {
		o.Current.InputVersion = spec.InputVersion
		log.Info("Commit object version for ValueState {{name}}", "name", o.Name)
		log.Info("  object version {{version}}", "version", o.Target.ObjectVersion)
		o.Current.ObjectVersion = o.Target.ObjectVersion
		o.Current.OutputVersion = spec.State.(*ValueOutputState).GetOutputVersion()
		o.Current.Output.Value = spec.State.(*ValueOutputState).GetState().Value

		log.Info("  provider {{provider}}", "provider", o.Target.Spec.ValueStateSpec.Provider)
		o.Current.Provider = o.Target.Spec.ValueStateSpec.Provider
	} else {
		log.Info("nothing to commit for phase {{phase}} of ValueState {{name}}")
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

func (c *CurrentValueState) get() *db.ValueCurrentState {
	return &c.n.GetBase().(*db.ValueState).Current
}

func (c *CurrentValueState) GetLinks() []model.ElementId {
	var r []model.ElementId

	if c.get().Provider != "" {
		r = append(r, common.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), c.get().Provider, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentValueState) GetInputVersion() string {
	return c.get().InputVersion
}

func (c *CurrentValueState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *CurrentValueState) GetOutputVersion() string {
	return c.get().OutputVersion
}

func (c *CurrentValueState) GetProvider() string {
	return c.get().Provider
}

func (c *CurrentValueState) GetOutput() model.OutputState {
	return NewValueOutputState(c.get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetValueState struct {
	n *ValueState
}

var _ model.TargetState = (*TargetValueState)(nil)

func (c *TargetValueState) get() *db.ValueTargetState {
	return c.n.GetBase().(*db.ValueState).Target
}

func (c *TargetValueState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.get()
	if t == nil {
		return nil
	}

	if t.Spec.Provider != "" {
		r = append(r, common.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.n.GetNamespace(), t.Spec.Provider, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetValueState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetValueState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetValueState) GetProvider() string {
	return c.get().Spec.Provider
}

func (c *TargetValueState) GetValue() int {
	return c.get().Spec.Value
}
