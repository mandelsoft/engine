package controllers

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/expression"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func Validate(o *db.Expression) (map[string]*ExpressionInfo, []string, error) {
	if len(o.Spec.Operands) == 0 && len(o.Spec.Expressions) > 0 {
		return nil, nil, fmt.Errorf("no operand specified")
	}
	exprs := map[string]*ExpressionInfo{}
	for n, e := range o.Spec.Expressions {
		switch e.Operator {
		case db.OP_ADD, db.OP_SUB, db.OP_MUL, db.OP_DIV:
			if e.Expression != "" {
				return nil, nil, fmt.Errorf("complex expression not possible for simple operator %q for expression %q", e.Operator, n)
			}
			if len(e.Operands) == 0 {
				return nil, nil, fmt.Errorf("nomoperands for %s expression %q", e.Operator, n)
			}
			for _, a := range e.Operands {
				_, ex := o.Spec.Expressions[a]
				if _, ok := o.Spec.Operands[a]; !ok && !ex {
					_, err := strconv.Atoi(a)
					if err != nil {
						return nil, nil, fmt.Errorf("operand %q for expression %q not found", a, n)
					}
				}
			}
			exprs[n] = &ExpressionInfo{
				Operator: e.Operator,
				Operands: e.Operands,
			}
		case db.OP_EXPR:
			if e.Expression == "" {
				return nil, nil, fmt.Errorf("complex expression required for operator %q in expression %q", e.Operator, n)
			}
			if len(e.Operands) != 0 {
				return nil, nil, fmt.Errorf("explicit operands not possible for complex expression in expression %q", n)
			}
			node, err := expression.Parse(e.Expression)
			if err != nil {
				return nil, nil, fmt.Errorf("complex expression in expression %q is invalid: %w", n, err)
			}
			operands := node.Operands()
			for _, a := range operands {
				_, ex := o.Spec.Expressions[a]
				if _, ok := o.Spec.Operands[a]; !ok && !ex {
					_, err := strconv.Atoi(a)
					if err != nil {
						return nil, nil, fmt.Errorf("operand %q for expression %q not found", a, n)
					}
				}
			}
			exprs[n] = &ExpressionInfo{
				Operator: e.Operator,
				Node:     node,
				Operands: operands,
			}
		default:
			return nil, nil, fmt.Errorf("invalid operator %q for expression %q", e.Operator, n)
		}
	}

	for n, v := range o.Spec.Operands {
		exprs[n] = &ExpressionInfo{
			Value: utils.Pointer(v),
		}
	}

	order, err := Order(exprs)
	return exprs, order, err
}

func PreCalc(log logging.Logger, order []string, elems map[string]*ExpressionInfo, values Values) error {
	log.Info("precalculation expressions")
outer:
	for _, n := range order {
		if _, ok := values[n]; ok {
			continue
		}
		info := elems[n]
		if info.Value != nil {
			values[n] = *info.Value
			continue
		}
		if info.Operator == db.OP_EXPR {
			continue
		}
		for _, o := range info.Operands {
			if _, ok := values.Get(o); !ok {
				info.Node = mapExpr(info, values)
				info.Operator = db.OP_EXPR
				continue outer
			}
		}
		v, err := calc(n, values, info)
		if err != nil {
			log.Info("- {{name}}({{expression}}) failed: {{error}}", "expression", info.String(), "name", n, "error", err)
			return err
		}
		log.Info("- {{name}}({{expression}}) = {{value}}", "expression", info.String(), "name", n, "value", v)
		values[n] = v
	}
	return nil
}

func Gather(log logging.Logger, odb database.Database[db2.DBObject], namespace string, elems map[string]*ExpressionInfo, values Values) error {
	log.Info("gathering graph outputs")
	for n := range elems {
		if _, ok := values[n]; ok {
			continue
		}
		id := database.NewObjectId(mymetamodel.TYPE_VALUE, namespace, n)
		o, err := odb.GetObject(id)
		if err != nil {
			log.Warn("cannot get graph object {{oid}}: {{error}}", "oid", id, "error", err)
			return err
		}
		values[n] = o.(*db.Value).Spec.Value
		log.Info("- found {{value}} for {{name}} ({{oid}})", "value", values[n], "name", n, "oid", id)
	}
	return nil
}

func mapExpr(d *ExpressionInfo, values Values) *expression.Node {
	var ops []*expression.Node

	if d.Node != nil {
		return d.Node
	}

	for _, o := range d.Operands {
		v, ok := values.Get(o)
		if ok {
			ops = append(ops, expression.NewValueNode(v))
		} else {
			ops = append(ops, expression.NewOperandNode(o))
		}
	}
	return expression.NewOperatorNode(operatorMap[d.Operator], ops...)
}

var operatorMap = map[db.OperatorName]string{
	db.OP_ADD: "+",
	db.OP_SUB: "-",
	db.OP_MUL: "*",
	db.OP_DIV: "/",
}

func Order(elems map[string]*ExpressionInfo) ([]string, error) {
	var order []string

	list := utils.OrderedMapKeys(elems)
outer:
	for _, n := range list {
		if c := cycle(elems, n); c != nil {
			return nil, fmt.Errorf("dependency cycle for %q: %s", n, strings.Join(c, "->"))
		}
		for i, o := range order {
			if slices.Contains(elems[o].Operands, n) {
				order = append(order[:i+1], order[i:]...)
				order[i] = n
				continue outer
			}
		}
		order = append(order, n)
	}
	return order, nil
}

func cycle(exprs map[string]*ExpressionInfo, n string, stack ...string) []string {
	c := utils.Cycle(n, stack...)
	if c == nil {
		stack = append(stack, n)
		for _, d := range exprs[n].Operands {
			if _, err := strconv.Atoi(d); err != nil {
				c = cycle(exprs, d, stack...)
				if c != nil {
					break
				}
			}
		}
	}
	return c
}

func calc(n string, values Values, info *ExpressionInfo) (int, error) {
	if info.Operator == db.OP_EXPR {
		return 0, fmt.Errorf("oops: calc called on external expression")
	}
	if len(info.Operands) == 0 {
		return 0, fmt.Errorf("no operands for %s", info.Operator)
	}
	var operands []int
	for _, a := range info.Operands {
		v, ok := values.Get(a)
		if !ok {
			return 0, fmt.Errorf("value for %q not yet available", a)
		}
		operands = append(operands, v)
	}
	r := operands[0]
	switch info.Operator {
	case db.OP_ADD:
		for _, v := range operands[1:] {
			r += v
		}
	case db.OP_SUB:
		for _, v := range operands[1:] {
			r -= v
		}
	case db.OP_MUL:
		for _, v := range operands[1:] {
			r *= v
		}
	case db.OP_DIV:
		for _, v := range operands[1:] {
			if v == 0 {
				return 0, fmt.Errorf("division by zero for operation %q", n)
			}
			r /= v
		}
	}
	return r, nil
}
