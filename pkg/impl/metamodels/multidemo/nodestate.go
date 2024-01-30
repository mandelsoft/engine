package demo

import (
	"fmt"
	"reflect"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalObjectSupport
}

type NodeStateCurrent struct {
	Result int `json:"result"`
}

var _ model.InternalObject = (*NodeState)(nil)

func (n *NodeState) GetCurrentState(phase model.Phase) model.CurrentState {
	return &CurrentGatherState{n}
}

func (n *NodeState) GetTargetState(phase model.Phase) model.TargetState {
	return &TargetCalcState{n}
}

func (n *NodeState) SetExternalState(ob objectbase.Objectbase, phase model.Phase, state common.ExternalStates) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.NodeState)
		mod := false
		for _, s := range state {
			mod = mod || n.setExternalObjectState(o, s.(*ExternalNodeState))
			switch phase {
			case multidemo.PHASE_GATHER:
				mod = mod || n.setExternalGatherState(o)
			case multidemo.PHASE_CALCULATION:
				mod = mod || n.setExternalCalcState(o, s.(*ExternalNodeState))
			}
		}
		return mod, mod
	})
	return err
}

func (n *NodeState) setExternalObjectState(o *db.NodeState, state *ExternalNodeState) bool {
	t := o.Target
	if t != nil {
		return false // keep state from first touched phase
	}
	t = &db.ObjectTargetState{}

	mod := false
	s := state.GetState()
	m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != state.GetVersion()
	if m {
		t.Spec = *s
		t.ObjectVersion = state.GetVersion()
	}
	mod = mod || m

	o.Target = t
	return mod
}

func (n *NodeState) setExternalGatherState(o *db.NodeState) bool {
	t := o.Gather.Target
	if t == nil {
		t = &db.GatherTargetState{}
	}

	mod := false
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, &mod)
	o.Gather.Target = t
	return mod
}

func (n *NodeState) setExternalCalcState(o *db.NodeState, state *ExternalNodeState) bool {
	t := o.Calculation.Target
	if t == nil {
		t = &db.CalculationTargetState{}
	}

	mod := false
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, &mod)
	o.Calculation.Target = t
	return mod
}

func (n *NodeState) Process(ob objectbase.Objectbase, req model.Request) model.Status {
	switch req.Element.GetPhase() {
	case multidemo.PHASE_GATHER:
		return n.processGather(ob, req)
	case multidemo.PHASE_CALCULATION:
		return n.processGather(ob, req)
	}
	return model.Status{
		Status: common.STATUS_FAILED,
		Error:  fmt.Errorf("unknoen phase %q", req.Element.GetPhase()),
	}
}

func (n *NodeState) processGather(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logger

	err := n.Validate()
	if err != nil {
		return model.Status{
			Status: common.STATUS_FAILED, // final failure
			Error:  err,
		}
	}

	links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
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
				Value:  *n.GetTargetState(req.Element.GetPhase()).(*TargetGatherState).GetValue(),
			},
		}
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: db.NewGatherResultState(operands),
	}
}

func (n *NodeState) processCalc(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logger

	err := n.Validate()
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
	s := n.GetTargetState(req.Element.GetPhase()).(*TargetCalcState)
	op := s.GetOperator()

	out := operands[0].Value
	if op != nil {
		log.Info("calculate %s%#v", *op, operands)
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
		log.Info("use input value %d", out)
	}

	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: db.NewCalcResultState(out),
	}
}

func (n *NodeState) Validate() error {
	_s := n.GetTargetState(multidemo.PHASE_GATHER)
	if _s == nil {
		return nil
	}
	s := _s.(*TargetGatherState)

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

type CurrentGatherState struct {
	n *NodeState
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) get() *db.GatherCurrentState {
	return &c.n.GetBase().(*db.NodeState).Gather.Current
}

func (c *CurrentGatherState) GetLinks() []model.ElementId {
	var r []model.ElementId

	for _, o := range c.n.GetBase().(*db.NodeState).Target.Spec.Operands {
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, multidemo.PHASE_GATHER))
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
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), multidemo.PHASE_GATHER)}
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
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, multidemo.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetGatherState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetGatherState) GetOperator() *db.Operator {
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
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), multidemo.PHASE_GATHER)}
}

func (c *TargetCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetCalcState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetCalcState) GetOperator() *db.Operator {
	return c.n.GetBase().(*db.NodeState).Target.Spec.Operator
}
