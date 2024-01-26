package demo

import (
	"fmt"
	"reflect"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/metamodels/demo"
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
	return &CurrentState{n}
}

func (n *NodeState) GetTargetState(phase model.Phase) model.TargetState {
	return &TargetState{n}
}

func (n *NodeState) SetExternalState(ob objectbase.Objectbase, phase model.Phase, state common.ExternalStates) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		t := _o.(*db.NodeState).Target
		if t == nil {
			t = &db.TargetState{}
		}

		mod := false
		for _, _s := range state { // we have just one external object here, but just for demonstration
			s := _s.(*ExternalNodeState).GetState()
			m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != _s.GetVersion()
			if m {
				t.Spec = *s
				t.ObjectVersion = _s.GetVersion()
			}
			mod = mod || m
		}

		_o.(*db.NodeState).Target = t
		return mod, mod
	})
	return err
}

func (n *NodeState) Process(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logger

	err := n.Validate()
	if err != nil {
		return model.Status{
			Status:      common.STATUS_FAILED, // final failure
			ResultState: nil,
			Error:       err,
		}
	}

	links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
	operands := make([]int, len(links))
	origin := make([]model.ObjectId, len(links))
	for iid, e := range req.Inputs {
		s := e.(*CurrentState)
		for i, oid := range links {
			if iid == oid {
				operands[i] = s.GetOutput()
				origin[i] = iid.ObjectId()
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i])
				break
			}
		}
	}
	s := n.GetTargetState(req.Element.GetPhase()).(*TargetState)
	op := s.GetOperator()

	out := 0
	if op != nil {
		out = operands[0]
		switch *op {
		case db.OP_ADD:
			for _, v := range operands[1:] {
				out += v
			}
		case db.OP_SUB:
			for _, v := range operands[1:] {
				out -= v
			}
		case db.OP_MUL:
			for _, v := range operands[1:] {
				out *= v
			}
		case db.OP_DIV:
			for i, v := range operands[1:] {
				if v == 0 {
					return model.Status{
						Status: common.STATUS_FAILED,
						Error:  fmt.Errorf("division by zero for operand %d[%s]", i, origin[i+1]),
					}
				}
				out /= v
			}
		}
	} else {
		out = *s.GetValue()
	}

	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: db.NewResultState(out),
	}
}

func (n *NodeState) Validate() error {
	_s := n.GetTargetState(demo.PHASE_UPDATING)
	if _s == nil {
		return nil
	}
	s := _s.(*TargetState)

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

type CurrentState struct {
	n *NodeState
}

var _ model.CurrentState = (*CurrentState)(nil)

func (c *CurrentState) get() *db.NodeState {
	return c.n.GetBase().(*db.NodeState)
}

func (c *CurrentState) GetLinks() []model.ElementId {
	var r []model.ElementId

	for _, o := range c.get().Current.Operands {
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, demo.PHASE_UPDATING))
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
	n *NodeState
}

var _ model.TargetState = (*TargetState)(nil)

func (c *TargetState) get() *db.NodeState {
	return c.n.GetBase().(*db.NodeState)
}

func (c *TargetState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.get().Target
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, common.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, demo.PHASE_UPDATING))
	}
	return r
}

func (c *TargetState) GetObjectVersion() string {
	return c.get().Target.ObjectVersion
}

func (c *TargetState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetState) GetOperator() *db.Operator {
	return c.get().Target.Spec.Operator
}

func (c *TargetState) GetValue() *int {
	return c.get().Target.Spec.Value
}
