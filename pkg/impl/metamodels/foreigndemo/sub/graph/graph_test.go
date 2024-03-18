package graph_test

import (
	"bytes"
	"fmt"
	"strings"

	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/version"
)

var NS = "testspace"

var _ = Describe("Test Environment", func() {
	Context("", func() {
		It("", func() {

			vA := graph.NewValue(db.NewValueNode(NS, "A", 5))
			vB := graph.NewValue(db.NewValueNode(NS, "B", 6))
			opC := graph.NewOperator(db.NewOperatorNode(NS, "C").
				AddOperand("iA", vA.GetName()).
				AddOperand("iB", vB.GetName()).
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "opE"),
			)

			g := Must(graph.NewGraph(version.Composed, vA, vB, opC))

			buf := bytes.NewBuffer(nil)
			MustBeSuccessful(g.Dump(buf))

			fmt.Printf("\n%s\n", buf)
			Expect(buf.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/C-A (
  OperatorState:Exposing/C (
    ExpressionState:Calculating/C (
      OperatorState:Gathering/C (
        ValueState:Propagating/A,
        ValueState:Propagating/B
      )
    ),
    OperatorState:Gathering/C (
      ValueState:Propagating/A,
      ValueState:Propagating/B
    )
  )
)
ValueState:Propagating/A[36060134d807a85bd4de45d03754fc36d6f73b2349587345f0f71b5548caeaee]
ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
OperatorState:Gathering/C[efced5b9c13a96ccafd0f4156e41f2b1800fcf9cdbbb6cad4a9a19816ded1b88]
`)))

			for _, n := range g.Nodes() {
				fmt.Printf("%s: %s\n", n, g.FormalVersion(n))
			}
		})
	})
})
