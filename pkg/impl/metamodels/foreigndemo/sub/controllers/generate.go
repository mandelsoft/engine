package controllers

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/expression"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/logging"
)

func Generate(log logging.Logger, namespace string, infos map[string]*ExpressionInfo, values Values) (graph.Graph, error) {
	nodes := map[string]graph.Node{}
	for n, e := range infos {
		if e.Operator != db.OP_EXPR {
			continue
		}

		generate(log, namespace, values, n, e.Node, nodes, 0)
	}
	return graph.NewGraph(version.Composed, maputils.Values(nodes)...)
}
func generate(log logging.Logger, namespace string, values map[string]int, base string, n *expression.Node, nodes map[string]graph.Node, index int) (string, int) {
	if n.Value != nil {
		return "", index
	}
	if len(n.Parents) == 0 {
		var node graph.Node
		if v, ok := values[n.Name]; ok {
			log.Info("- generate value node {{name}} for {{value}}", "name", n.Name, "value", v)
			node = graph.NewValue(db.NewValueNode(namespace, n.Name, v))
			nodes[n.Name] = node
			return node.GetName(), index
		}
		log.Info("- using result node {{name}}", "name", n.Name)
		return n.Name, index
	}

	gen := index + 1

	name := base
	if index > 0 {
		name = fmt.Sprintf("%s-%d", base, index)
	}
	log.Info("- generate operator node {{name}}", "name", name)
	op := db.NewOperatorNode(namespace, name).AddOutput(name, "E")
	var ops []string
	for i, p := range n.Parents {
		var opNode string
		opNode, gen = generate(log, namespace, values, base, p, nodes, gen)

		in := fmt.Sprintf("O%d", i+1)
		if opNode == "" {
			op.AddOperand(in, fmt.Sprintf("%d", *p.Value))
		} else {
			op.AddOperand(in, opNode)
		}
		ops = append(ops, in)
	}
	op.AddOperation("E", operators[n.Name], ops...)

	node := graph.NewOperator(op)
	nodes[name] = node
	return node.GetName(), gen
}

var operators = map[string]db.OperatorName{
	"+": db.OP_ADD,
	"-": db.OP_SUB,
	"*": db.OP_MUL,
	"/": db.OP_DIV,
}
