package expression

import (
	"fmt"
	"slices"
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
		return fmt.Sprintf("(%s%s%s)", n.Parents[0], n.Name, n.Parents[1])
	}
	return n.Name
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
