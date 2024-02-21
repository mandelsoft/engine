package multidemo

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalPhaseObjectSupport[*NodeState, *db.NodeState, *ExternalNodeState]
}

var _ runtime.InitializedObject = (*NodeState)(nil)

func (n *NodeState) Initialize() error {
	return support.SetSelf(n, nodeStatePhases, db.NodePhaseStateAccess)
}

var _ model.InternalObject = (*NodeState)(nil)

var nodeStatePhases = support.NewPhases[*NodeState, *db.NodeState, *ExternalNodeState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type NodeStatePhase = support.Phase[*NodeState, *db.NodeState, *ExternalNodeState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct {
	support.DefaultPhase[*NodeState, *db.NodeState]
}

func (_ PhaseBase) AcceptExternalState(log logging.Logger, o *NodeState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	for _, _s := range state {
		s := _s.(*ExternalNodeState).GetState()

		op := s.Operator
		if op != nil && s.Value != nil {
			return model.ACCEPT_INVALID, fmt.Errorf("only operator or value can be specified")
		}
		if op == nil && s.Value == nil {
			return model.ACCEPT_INVALID, fmt.Errorf("operator or value must be specified")
		}

		if op != nil {
			if len(s.Operands) == 0 {
				return model.ACCEPT_INVALID, fmt.Errorf("operator node requires at least one operand")
			}
			switch *op {
			case db.OP_ADD:
			case db.OP_SUB:
			case db.OP_DIV:
			case db.OP_MUL:
			default:
				return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q", *op)
			}
		} else {
			if len(s.Operands) != 0 {
				return model.ACCEPT_INVALID, fmt.Errorf("operands only possible for operator node")
			}
		}
	}
	return model.ACCEPT_OK, nil
}

type GatherPhase struct{ PhaseBase }

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

var _ NodeStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) GetCurrentState(o *NodeState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (_ GatherPhase) GetTargetState(o *NodeState, phase Phase) model.TargetState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase Phase, state *ExternalNodeState, mod *bool) {
	t := o.Gather.Target
	support.UpdateField(&t.Spec, state.GetState(), mod)
}

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.NodeState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		t := o.Gather.Target
		c := &o.Gather.Current
		// update phase specific state
		log.Info("  operands {{operands}}", "operands", t.Spec.Operands)
		c.Operands = t.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c.Output.Values = spec.OutputState.(*GatherOutputState).GetState()
	}
}

func (p GatherPhase) Process(o *NodeState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	links := p.GetTargetState(o, phase).GetLinks()
	operands := make([]db.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*CalcOutputState).GetState()
		for i, oid := range links {
			if iid == oid {
				operands[i] = db.Operand{
					Origin: iid.ObjectId(),
					Value:  s,
				}
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i].Value)
				break
			}
		}
	}

	if len(links) == 0 {
		operands = []db.Operand{
			{
				Origin: mmids.NewObjectIdFor(req.Element.GetObject()),
				Value:  *(NewTargetGatherState(o)).GetValue(),
			},
		}
	}
	return model.StatusCompleted(NewGatherOutputState(operands))
}

////////////////////////////////////////////////////////////////////////////////
// Calculation Phase
////////////////////////////////////////////////////////////////////////////////

type CalculatePhase struct{ PhaseBase }

var _ NodeStatePhase = (*CalculatePhase)(nil)

func (_ CalculatePhase) GetCurrentState(o *NodeState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (_ CalculatePhase) GetTargetState(o *NodeState, phase Phase) model.TargetState {
	return NewTargetCalcState(o)
}

func (p CalculatePhase) AcceptExternalState(log logging.Logger, o *NodeState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	for _, s := range state {
		if s.(*ExternalNodeState).GetVersion() == o.GetPhaseState(mymetamodel.PHASE_GATHER).GetCurrent().GetObjectVersion() {
			return p.PhaseBase.AcceptExternalState(log, o, state, phase)
		}
	}
	return model.ACCEPT_REJECTED, fmt.Errorf("gather phase not up to date")
}

func (_ CalculatePhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase Phase, state *ExternalNodeState, mod *bool) {
	t := o.Calculation.Target
	support.UpdatePointerField(&t.Operator, state.GetState().Operator, mod)
}

func (_ CalculatePhase) DBCommit(log logging.Logger, o *db.NodeState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		// update state specific
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		c := &o.Calculation.Current
		c.Output.Value = spec.OutputState.(*CalcOutputState).GetState()
	}
}

func (_ CalculatePhase) Process(o *NodeState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*GatherOutputState).GetState()
	}
	s := NewTargetCalcState(o)
	op := s.GetOperator()

	out := operands[0].Value
	if op != nil {
		log.Info("calculate {{operator}} {{operands}}", "operator", *op, "operands", operands)
		switch *op {
		case db.OP_ADD:
			for _, v := range operands[1:] {
				out += v.Value
			}
		case db.OP_SUB:
			for _, v := range operands[1:] {
				out -= v.Value
			}
		case db.OP_MUL:
			for _, v := range operands[1:] {
				out *= v.Value
			}
		case db.OP_DIV:
			for i, v := range operands[1:] {
				if v.Value == 0 {
					return model.StatusFailed(fmt.Errorf("division by zero for operand %d[%s]", i, operands[i+1].Origin))
				}
				out /= v.Value
			}
		}
	} else {
		log.Info("use input value {{input}}}", "input", out)
	}

	return model.StatusCompleted(NewCalcOutputState(out))
}

////////////////////////////////////////////////////////////////////////////////

type NodeStateCurrent struct {
	Result int `json:"result"`
}

///////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[[]db.Operand]
type CalcOutputState = support.OutputState[int]

var NewGatherOutputState = support.NewOutputState[[]db.Operand]
var NewCalcOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	support.CurrentStateSupport[*db.NodeState, *db.GatherCurrentState]
}

func NewCurrentGatherState(n *NodeState) model.CurrentState {
	return &CurrentGatherState{support.NewCurrentStateSupport[*db.NodeState, *db.GatherCurrentState](n, mymetamodel.PHASE_GATHER)}
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.Get().Operands {
		r = append(r, mmids.NewElementId(c.GetType(), c.GetNamespace(), o, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.Get().Output.Values)
}

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.NodeState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *NodeState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.NodeState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.Get().Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	support.TargetStateSupport[*db.NodeState, *db.GatherTargetState]
}

var _ model.TargetState = (*TargetGatherState)(nil)

func NewTargetGatherState(n *NodeState) *TargetGatherState {
	return &TargetGatherState{support.NewTargetStateSupport[*db.NodeState, *db.GatherTargetState](n, mymetamodel.PHASE_GATHER)}
}

func (c *TargetGatherState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, mmids.NewElementId(c.GetType(), c.GetNamespace(), o, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetGatherState) GetOperator() *db.OperatorName {
	return c.Get().Spec.Operator
}

func (c *TargetGatherState) GetValue() *int {
	return c.Get().Spec.Value
}

////////////////////////////////////////////////////////////////////////////////

type TargetCalcState struct {
	support.TargetStateSupport[*db.NodeState, *db.CalculationTargetState]
}

var _ model.TargetState = (*TargetCalcState)(nil)

func NewTargetCalcState(n *NodeState) *TargetCalcState {
	return &TargetCalcState{support.NewTargetStateSupport[*db.NodeState, *db.CalculationTargetState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *TargetCalcState) GetLinks() []mmids.ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *TargetCalcState) GetOperator() *db.OperatorName {
	return c.Get().Operator
}
