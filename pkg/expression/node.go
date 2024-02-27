package expression

import (
	"fmt"
	"slices"

	"github.com/mandelsoft/engine/pkg/utils"
)

type Node struct {
	Name    string
	Parents []*Node
	Value   *int
}

func (n *Node) String() string {

	if n.Value != nil {
		return fmt.Sprintf("%d", *n.Value)
	}
	if len(n.Parents) > 0 {
		s := ""
		sep := ""
		for _, p := range n.Parents {
			s = fmt.Sprintf("%s%s%s", s, sep, p)
			sep = n.Name
		}
		return fmt.Sprintf("(%s)", s)
	}
	return n.Name
}

func NewValueNode(v int) *Node {
	return &Node{
		Value: utils.Pointer(v),
	}
}

func NewOperandNode(n string) *Node {
	return &Node{
		Name: n,
	}
}

func NewOperatorNode(op string, ops ...*Node) *Node {
	return &Node{
		Name:    op,
		Parents: ops,
	}
}

func (n *Node) Operands() []string {
	if n.Value != nil {
		return nil
	}
	if len(n.Parents) > 0 {
		var result []string
		for _, p := range n.Parents {
			for _, o := range p.Operands() {
				if !slices.Contains(result, o) {
					result = append(result, o)
				}
			}
		}
		return result
	}
	return []string{n.Name}
}
