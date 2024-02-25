package simple

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

type CalculatePhase struct{ PhaseBase }

var _ OperatorStatePhase = (*CalculatePhase)(nil)

func (c CalculatePhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (c CalculatePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetCalcState(o)
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	support.UpdateField(&o.Calculation.Target.ObjectVersion, &o.Gather.Current.ObjectVersion, mod)
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Calculation.Target != nil && spec != nil {
		c := &o.Calculation.Current
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		support.UpdateField(&c.Output, utils.Pointer(spec.OutputState.(*CalcOutputState).GetState()), mod)
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (c CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	if req.Delete {
		log.Info("deletion successful")
		return model.StatusDeleted()
	}
	s := NewTargetCalcState(o)

	out := db.CalculationOutput{}
	values := map[string]int{}

	log.Info("preparing effective value set")
	op := req.Inputs[s.PhaseLink(mymetamodel.PHASE_GATHER)].(*GatherOutputState).GetState()
	for n, o := range op.Operands {
		values[n] = o.Value
	}
	log.Info("- value set from inputs: {{values}}", "values", values)
	ex := req.Inputs[s.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)].(*EvaluationOutputState).GetState()
	log.Info("- value set from expressions: {{values}}", "values", ex)
	for n, v := range ex {
		values[n] = v
	}

	// calculate value schedule
	log.Info("preparing {{amount}} outbound assignments", "amount", len(op.Outputs))
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

	return model.StatusCompleted(NewCalcOutputState(req.FormalVersion, out))
}

func (_ CalculatePhase) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o *OperatorState, phase Phase) error {
	s := NewCurrentCalcState(o)

	for k := range s.GetOutput().(*CalcOutputState).GetState() {
		oid := database.NewObjectId(mymetamodel.TYPE_VALUE, o.GetNamespace(), k)
		err := support.RequestSlaveDeletion(log, ob, oid)
		if err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type CalcOutputState = support.OutputState[db.CalculationOutput]

var NewCalcOutputState = support.NewOutputState[db.CalculationOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *OperatorState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.GetFormalVersion(), c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetCalcState struct {
	support.TargetStateSupport[*db.OperatorState, *db.CalculationTargetState]
}

var _ model.TargetState = (*TargetCalcState)(nil)

func NewTargetCalcState(n *OperatorState) *TargetCalcState {
	return &TargetCalcState{support.NewTargetStateSupport[*db.OperatorState, *db.CalculationTargetState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *TargetCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)}
}
