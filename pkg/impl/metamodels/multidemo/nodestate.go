package multidemo

import (
	"fmt"
	"reflect"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
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
	return support.SetSelf(n, nodeStatePhases)
}

var _ model.InternalObject = (*NodeState)(nil)

var nodeStatePhases = support.NewPhases[*NodeState, *db.NodeState, *ExternalNodeState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type Phase = support.Phase[*NodeState, *db.NodeState, *ExternalNodeState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct{}

func (c PhaseBase) setExternalObjectState(log logging.Logger, o *db.NodeState, state *ExternalNodeState, mod *bool) {
	t := o.Target
	if t != nil {
		return // keep state from first touched phase
	}
	log.Info("set common target state for NodeState {{name}}")
	t = &db.ObjectTargetState{}

	s := state.GetState()
	m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != state.GetVersion()
	if m {
		t.Spec = *s
		t.ObjectVersion = state.GetVersion()
	}
	*mod = *mod || m

	o.Target = t
}

func (g PhaseBase) Validate(o *NodeState) error {
	s := TargetGatherState{o}

	op := s.GetOperator()
	if op != nil && s.GetValue() != nil {
		return fmt.Errorf("only operator or value can be specified")
	}
	if op == nil && s.GetValue() == nil {
		return fmt.Errorf("operator or value must be specified")
	}

	if op != nil {
		if len(s.GetLinks()) == 0 {
			return fmt.Errorf("operator node requires at least one operand")
		}
		switch *op {
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return fmt.Errorf("unknown operator %q", *op)
		}
	} else {
		if len(s.GetLinks()) != 0 {
			return fmt.Errorf("operands only possible for operator node")
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ Phase = (*GatherPhase)(nil)

func (g GatherPhase) GetCurrentState(o *NodeState, phase model.Phase) model.CurrentState {
	return &CurrentGatherState{o}
}

func (g GatherPhase) GetTargetState(o *NodeState, phase model.Phase) model.TargetState {
	return &TargetGatherState{o}
}

func (g GatherPhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase model.Phase, state *ExternalNodeState, mod *bool) {
	g.setExternalObjectState(log, o, state, mod)
	t := o.Gather.Target
	if t == nil {
		t = &db.GatherTargetState{}
	}

	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Gather.Target = t
}

func (g GatherPhase) DBCommit(log logging.Logger, o *db.NodeState, phase model.Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		log.Info("commit phase {{phase}} for NodeState {{name}}")
		log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
		log.Info("  object version {{objvers}}", "objvers", o.Gather.Target.ObjectVersion)
		log.Info("  output version {{outvers}}", "outvers", spec.State.(*GatherResultState).GetOutputVersion())
		log.Info("  output {{output}}", "output", spec.State.(*GatherResultState).GetState())
		c := &o.Gather.Current
		c.InputVersion = spec.InputVersion
		c.ObjectVersion = o.Gather.Target.ObjectVersion
		c.OutputVersion = spec.State.(*GatherResultState).GetOutputVersion()
		c.Output.Values = spec.State.(*GatherResultState).GetState()
	}
	o.Gather.Target = nil
}

func (g GatherPhase) Process(ob objectbase.Objectbase, o *NodeState, phase model.Phase, req model.Request) model.Status {
	log := req.Logging.Logger(REALM)

	err := g.Validate(o)
	if err != nil {
		return model.Status{
			Status: common.STATUS_FAILED, // final failure
			Error:  err,
		}
	}

	links := g.GetTargetState(o, phase).GetLinks()
	operands := make([]db.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*CurrentCalcState)
		for i, oid := range links {
			if iid == oid {
				operands[i] = db.Operand{
					Origin: iid.ObjectId(),
					Value:  s.GetOutput(),
				}
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i].Value)
				break
			}
		}
	}

	if len(links) == 0 {
		operands = []db.Operand{
			{
				Origin: common.NewObjectIdFor(req.Element.GetObject()),
				Value:  *(&TargetGatherState{o}).GetValue(),
			},
		}
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: NewGatherResultState(operands),
	}
}

////////////////////////////////////////////////////////////////////////////////
// Calculation Phase
////////////////////////////////////////////////////////////////////////////////

type CalculatePhase struct{ PhaseBase }

var _ Phase = (*CalculatePhase)(nil)

func (c CalculatePhase) GetCurrentState(o *NodeState, phase model.Phase) model.CurrentState {
	return &CurrentCalcState{o}
}

func (c CalculatePhase) GetTargetState(o *NodeState, phase model.Phase) model.TargetState {
	return &TargetCalcState{o}
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase model.Phase, state *ExternalNodeState, mod *bool) {
	c.setExternalObjectState(log, o, state, mod)
	t := o.Calculation.Target
	if t == nil {
		t = &db.CalculationTargetState{}
	}

	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Calculation.Target = t
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.NodeState, phase model.Phase, spec *model.CommitInfo, mod *bool) {
	if o.Calculation.Target != nil && spec != nil {
		// update state specific
		log.Info("commit phase {{phase}} for NodeState {{name}}")
		log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
		log.Info("  object version {{objvers}}", "objvers", o.Calculation.Target.ObjectVersion)
		log.Info("  output version {{outvers}}", "outvers", spec.State.(*CalcResultState).GetOutputVersion())
		log.Info("  output {{output}}", "output", spec.State.(*CalcResultState).GetState())
		c := &o.Calculation.Current
		c.InputVersion = spec.InputVersion
		c.ObjectVersion = o.Calculation.Target.ObjectVersion
		c.OutputVersion = spec.State.(*CalcResultState).GetOutputVersion()
		c.Output.Value = spec.State.(*CalcResultState).GetState()

		// ... and common state for last phase
		log.Info("  operands {{operands}}", "operands", o.Target.Spec.Operands)
		o.Current.Operands = o.Target.Spec.Operands
	}
	o.Calculation.Target = nil
	o.Target = nil
}

func (c CalculatePhase) Process(ob objectbase.Objectbase, o *NodeState, phase model.Phase, req model.Request) model.Status {
	log := req.Logging.Logger(REALM)

	err := c.Validate(o)
	if err != nil {
		return model.Status{
			Status:      common.STATUS_FAILED, // final failure
			ResultState: nil,
			Error:       err,
		}
	}

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*CurrentGatherState).GetOutput()
	}
	s := &TargetCalcState{o}
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
					return model.Status{
						Status: common.STATUS_FAILED,
						Error:  fmt.Errorf("division by zero for operand %d[%s]", i, operands[i+1].Origin),
					}
				}
				out /= v.Value
			}
		}
	} else {
		log.Info("use input value {{input}}}", "input", out)
	}

	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: NewCalcResultState(out),
	}
}

////////////////////////////////////////////////////////////////////////////////

type NodeStateCurrent struct {
	Result int `json:"result"`
}

///////////////////////////////////////////////////////////////////////////////

type GatherResultState = support.ResultState[[]db.Operand]
type CalcResultState = support.ResultState[int]

var NewGatherResultState = support.NewResultState[[]db.Operand]
var NewCalcResultState = support.NewResultState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	n *NodeState
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) get() *db.GatherCurrentState {
	return &c.n.GetBase().(*db.NodeState).Gather.Current
}

func (c *CurrentGatherState) GetLinks() []model.ElementId {
	var r []model.ElementId

	for _, o := range c.n.GetBase().(*db.NodeState).Current.Operands {
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *CurrentGatherState) GetInputVersion() string {
	return c.get().InputVersion
}

func (c *CurrentGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *CurrentGatherState) GetOutputVersion() string {
	return c.get().OutputVersion
}

func (c *CurrentGatherState) GetOutput() []db.Operand {
	return c.get().Output.Values
}

type CurrentCalcState struct {
	n *NodeState
}

var _ model.CurrentState = (*CurrentCalcState)(nil)

func (c *CurrentCalcState) get() *db.CalculationCurrentState {
	return &c.n.GetBase().(*db.NodeState).Calculation.Current
}

func (c *CurrentCalcState) GetLinks() []model.ElementId {
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetInputVersion() string {
	return c.get().InputVersion
}

func (c *CurrentCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *CurrentCalcState) GetOutputVersion() string {
	return c.get().OutputVersion
}

func (c *CurrentCalcState) GetOutput() int {
	return c.get().Output.Value
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	n *NodeState
}

var _ model.TargetState = (*TargetGatherState)(nil)

func (c *TargetGatherState) get() *db.GatherTargetState {
	return c.n.GetBase().(*db.NodeState).Gather.Target
}

func (c *TargetGatherState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.n.GetBase().(*db.NodeState).Target
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetGatherState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetGatherState) GetOperator() *db.OperatorName {
	return c.n.GetBase().(*db.NodeState).Target.Spec.Operator
}

func (c *TargetGatherState) GetValue() *int {
	return c.n.GetBase().(*db.NodeState).Target.Spec.Value
}

type TargetCalcState struct {
	n *NodeState
}

var _ model.TargetState = (*TargetCalcState)(nil)

func (c *TargetCalcState) get() *db.CalculationTargetState {
	return c.n.GetBase().(*db.NodeState).Calculation.Target
}

func (c *TargetCalcState) GetLinks() []common.ElementId {
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *TargetCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetCalcState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetCalcState) GetOperator() *db.OperatorName {
	return c.n.GetBase().(*db.NodeState).Target.Spec.Operator
}
