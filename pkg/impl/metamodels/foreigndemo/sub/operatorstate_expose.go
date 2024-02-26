package sub

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

type ExposePhase struct{ PhaseBase }

var _ OperatorStatePhase = (*ExposePhase)(nil)

func (c ExposePhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (c ExposePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetExposeState(o)
}

func (c ExposePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	support.UpdateField(&o.Expose.Target.ObjectVersion, &o.Gather.Current.ObjectVersion, mod)
}

func (c ExposePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Expose.Target != nil && spec != nil {
		c := &o.Expose.Current
		log.Info("  output {{output}}", "output", spec.OutputState.(*ExposeOutputState).GetState())
		support.UpdateField(&c.Output, utils.Pointer(spec.OutputState.(*ExposeOutputState).GetState()), mod)
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (c ExposePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	if req.Delete {
		log.Info("deletion successful")
		return model.StatusDeleted()
	}
	s := NewTargetExposeState(o)

	out := db.ExposeOutput{}
	values := map[string]int{}

	log.Info("preparing effective value set")
	op := req.Inputs[s.PhaseLink(mymetamodel.PHASE_GATHER)].(*GatherOutputState).GetState()
	for n, o := range op.Operands {
		values[n] = o.Value
	}
	log.Info("- value set from inputs: {{values}}", "values", values)
	ex := req.Inputs[s.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE)].(*EvaluationOutputState).GetState()
	log.Info("- value set from expressions: {{values}}", "values", ex)
	for n, v := range ex {
		values[n] = v
	}

	// calculate value schedule
	log.Info("preparing outbound assignments")
	for i, e := range op.Outputs {
		v := values[e]
		out[i] = v
		log.Info("- {{outbound}}: {{value}}", "outbound", i, "value", v)
	}

	var slaves []ElementId
	for k := range op.Outputs {
		slaves = append(slaves, NewElementId(mymetamodel.TYPE_VALUE_STATE, req.Element.GetNamespace(), k, mymetamodel.PHASE_PROPAGATE))
	}

	req.SlaveManagement.AssureSlaves(
		func(i model.InternalObject) error {
			o := i.(support.InternalObject).GetBase().(*db.ValueState)
			if o.Spec.Provider != "" && o.Spec.Provider != req.Element.GetName() {
				return fmt.Errorf("target value object %q already served by operatpr %q", i.GetName(), req.Element.GetName())
			}
			return nil
		},
		support.SlaveCreationFunc(func(o *db.ValueState) (bool, bool) {
			mod := false
			support.UpdateField(&o.Spec.Provider, utils.Pointer(req.Element.GetName()), &mod)
			return mod, mod
		}),
		slaves...,
	)

	return model.StatusCompleted(NewExposeState(req.FormalVersion, out))
}

func (_ ExposePhase) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o *OperatorState, phase Phase) error {
	s := NewCurrentCalcState(o)

	for k := range s.GetOutput().(*ExposeOutputState).GetState() {
		oid := database.NewObjectId(mymetamodel.TYPE_VALUE, o.GetNamespace(), k)
		err := support.RequestSlaveDeletion(log, ob, oid)
		if err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type ExposeOutputState = support.OutputState[db.ExposeOutput]

var NewExposeState = support.NewOutputState[db.ExposeOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentExposeState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.ExposeCurrentState]
}

func NewCurrentCalcState(n *OperatorState) model.CurrentState {
	return &CurrentExposeState{support.NewCurrentStateSupport[*db.OperatorState, *db.ExposeCurrentState](n, mymetamodel.PHASE_EXPOSE)}
}

func (c *CurrentExposeState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE)}
}

func (c *CurrentExposeState) GetOutput() model.OutputState {
	return NewExposeState(c.GetFormalVersion(), c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetExposeState struct {
	support.TargetStateSupport[*db.OperatorState, *db.ExposeTargetState]
}

var _ model.TargetState = (*TargetExposeState)(nil)

func NewTargetExposeState(n *OperatorState) *TargetExposeState {
	return &TargetExposeState{support.NewTargetStateSupport[*db.OperatorState, *db.ExposeTargetState](n, mymetamodel.PHASE_EXPOSE)}
}

func (c *TargetExposeState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE)}
}
