package sub

import (
	"errors"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
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

func (n *ValueState) GetExternalState(o model.ExternalObject, phase Phase) model.ExternalState {
	// incorporate actual binding into state
	return n.EffectiveTargetSpec(o.GetState())
}

func (n *ValueState) assureTarget(o *db.ValueState) *db.ValueTargetState {
	return o.CreateTarget().(*db.ValueTargetState)
}

func (n *ValueState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase Phase, state model.ExternalState) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		t := n.assureTarget(_o.(*db.ValueState))

		mod := false
		if state == nil {
			// external object not existent
			state = n.EffectiveTargetSpec(nil)
		}
		s := state.(*EffectiveValueState).GetState()
		fv := "" // if used as slave it does not have an own formal object version, only a formal (graph) version.
		if s.Provider == "" {
			fv = support.NewState(s.ValueSpec).GetVersion()
		}
		mod = t.SetFormalObjectVersion(fv) || mod
		support.UpdateField(&t.Spec, s, &mod)
		support.UpdateField(&t.ObjectVersion, utils.Pointer(state.GetVersion()), &mod)
		return mod, mod
	})
	return 0, err
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

func (n *ValueState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	if req.Delete {
		log.Info("deleting successful")
		return model.StatusDeleted()
	}

	target := n.GetTargetState(req.Element.GetPhase())

	var out db.ValueOutput
	if len(req.Inputs) > 0 {
		links := target.GetLinks()
		for iid, e := range req.Inputs {
			s := e.(*ExposeOutputState).GetState()
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
		o, err := n.assureSlave(log, req.Model.ObjectBase(), &out)
		if err != nil {
			return model.StatusCompleted(nil, err)
		}
		modifiedObjectVersion := model.ModifiedSlaveObjectVersion(log, req.Element, o)
		return model.StatusCompleted(NewValueOutputState(req.FormalVersion, out)).ModifyObjectVersion(modifiedObjectVersion)
	}

	log.Info("provider {{provider}} does not feed value anymore", "provider", target.(*TargetValueState).GetProvider())
	log.Info("deleting slave value object")

	err := req.Model.ObjectBase().DeleteObject(database.NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName()))
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return model.StatusCompleted(nil, err)
		}
	}
	return model.StatusDeleted()
}

func (n *ValueState) assureSlave(log logging.Logger, ob objectbase.Objectbase, out *db.ValueOutput) (model.ExternalObject, error) {
	var modobj model.ExternalObject

	extid := database.NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName())
	log = log.WithValues("extid", extid)
	if *out.Origin != NewObjectId(mymetamodel.TYPE_VALUE, n.GetNamespace(), n.GetName()) {
		log.Info("checking slave value object {{extid}}")
		_, err := ob.GetObject(extid)
		if errors.Is(err, database.ErrNotExist) {
			log.Info("value object {{extid}} not found")
			_o, err := ob.CreateObject(extid)
			o := _o.(*Value)
			if err == nil {
				_, err = wrapped.Modify(ob, o, func(_o db2.Object) (bool, bool) {
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
				return nil, err
			}
			modobj = _o.(model.ExternalObject)
			log.Info("slave value object {{extid}} created")
		}
	}
	return modobj, nil
}

func (n *ValueState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.ValueState](n.commitTargetState))
}

func (n *ValueState) commitTargetState(lctx model.Logging, o *db.ValueState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*ValueOutputState).GetState())
		o.Current.Output.Value = spec.OutputState.(*ValueOutputState).GetState().Value
		log.Info("  provider {{provider}}", "provider", o.Target.Spec.ValueStateSpec.Provider)
		o.Current.Provider = o.Target.Spec.ValueStateSpec.Provider
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

func (c *CurrentValueState) GetLinks() []ElementId {
	var r []ElementId

	if c.Get().Provider != "" {
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), c.Get().Provider, mymetamodel.PHASE_EXPOSE))
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
		r = append(r, NewElementId(mymetamodel.TYPE_OPERATOR_STATE, c.GetNamespace(), t.Spec.Provider, mymetamodel.PHASE_EXPOSE))
	}
	return r
}

func (c *TargetValueState) GetProvider() string {
	return c.Get().Spec.Provider
}

func (c *TargetValueState) GetValue() int {
	return c.Get().Spec.Value
}

func (c *TargetValueState) AdjustObjectVersion(v string) {
	c.Get().ObjectVersion = v
}
