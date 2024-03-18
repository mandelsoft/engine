package sub

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

type ExposePhase struct{ PhaseBase }

var _ OperatorStatePhase = (*ExposePhase)(nil)

func (c ExposePhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentExposeState(o)
}

func (c ExposePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetExposeState(o)
}

func (c ExposePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	support.UpdateField(&o.Expose.Target.ObjectVersion, &o.Gather.Current.ObjectVersion, mod)
}

func (_ ExposePhase) DBRollback(log logging.Logger, o *db.OperatorState, phase Phase, mod *bool) {
}

func (c ExposePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		c := &o.Expose.Current
		log.Info("  output {{output}}", "output", spec.OutputState.(*ExposeOutputState).GetState())
		c.Output = spec.OutputState.(*ExposeOutputState).GetState()
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
			support.UpdateField(&o.Spec.Provider, generics.Pointer(req.Element.GetName()), &mod)
			if mod {
				log.Info("update provider for {{slaveid}} to {{provider}}", "slaveid", NewElementIdForPhase(o, mymetamodel.PHASE_PROPAGATE), req.Element.GetName())
			} else {
				log.Info("preserve provider {{provider}} for {{slaveid}}", "slaveid", NewElementIdForPhase(o, mymetamodel.PHASE_PROPAGATE), req.Element.GetName())
			}
			return mod, mod
		}),
		slaves...,
	)

	return model.StatusCompleted(NewExposeOutputState(req.FormalVersion, out))
}

func (_ ExposePhase) PrepareDeletion(log logging.Logger, mgmt model.SlaveManagement, o *OperatorState, phase Phase) error {
	s := NewCurrentExposeState(o)

	var eids []ElementId
	for k := range s.GetOutput().(*ExposeOutputState).GetState() {
		eids = append(eids, NewElementId(mymetamodel.TYPE_VALUE_STATE, o.GetNamespace(), k, mymetamodel.PHASE_PROPAGATE))
	}
	return mgmt.MarkForDeletion(eids...)
}

////////////////////////////////////////////////////////////////////////////////

type ExposeOutputState = support.OutputState[db.ExposeOutput]

var NewExposeOutputState = support.NewOutputState[db.ExposeOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentExposeState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.ExposeCurrentState]
}

func NewCurrentExposeState(n *OperatorState) model.CurrentState {
	return &CurrentExposeState{support.NewCurrentStateSupport[*db.OperatorState, *db.ExposeCurrentState](n, mymetamodel.PHASE_EXPOSE)}
}

func (c *CurrentExposeState) GetObservedState() model.ObservedState {
	return c.GetObservedStateForPhase(mymetamodel.PHASE_GATHER, c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE))
}

func (c *CurrentExposeState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE)}
}

func (c *CurrentExposeState) GetOutput() model.OutputState {
	return NewExposeOutputState(c.GetFormalVersion(), c.Get().Output)
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
