package controllers_test

import (
	"bytes"
	"fmt"
	"strings"

	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
)

var NS = "testspace"

var _ = Describe("Test Environment", func() {
	var buf *bytes.Buffer
	var log logging.Logger

	BeforeEach(func() {
		buf = bytes.NewBuffer(nil)
		log = logrusl.Human().WithWriter(buf).New().Logger()
	})

	Context("order", func() {
		It("cycle", func() {
			exprs := map[string]*controllers.ExpressionInfo{
				"A": controllers.NewExpressionInfo("B", "C"),
				"B": controllers.NewExpressionInfo("D"),
				"C": controllers.NewExpressionInfo("B"),
				"D": controllers.NewExpressionInfo("A"),
			}

			_, err := controllers.Order(exprs)
			Expect(err).To(MatchError("dependency cycle for \"A\": A->B->D->A"))
		})

		It("orders", func() {
			exprs := map[string]*controllers.ExpressionInfo{
				"A": controllers.NewExpressionInfo("B"),
				"B": controllers.NewExpressionInfo("D"),
				"C": controllers.NewExpressionInfo("B"),
				"D": controllers.NewExpressionInfo(),
			}

			list := Must(controllers.Order(exprs))

			Expect(list).To(Equal([]string{"D", "B", "A", "C"}))
		})

		It("orders", func() {
			exprs := map[string]*controllers.ExpressionInfo{
				"A": controllers.NewExpressionInfo("B", "C"),
				"B": controllers.NewExpressionInfo("D"),
				"C": controllers.NewExpressionInfo("B"),
				"D": controllers.NewExpressionInfo(),
			}

			list := Must(controllers.Order(exprs))

			Expect(list).To(Equal([]string{"D", "B", "C", "A"}))
		})
	})

	Context("generate", func() {
		It("map simple expression", func() {
			e := db.NewExpression(NS, "C").
				AddOperand("iA", 5).
				AddOperand("iB", 6).
				AddExpressionOperation("eA", "iA+iB")

			infos, order := Must2(controllers.Validate(e))
			values := map[string]int{}
			MustBeSuccessful(controllers.PreCalc(log, order, infos, values))
			g := Must(controllers.Generate(log, "ns", infos, values))

			desc := bytes.NewBuffer(nil)
			MustBeSuccessful(g.Dump(desc))
			fmt.Printf("\n%s\n", desc.String())
			Expect(desc.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/eA (
  OperatorState:Exposing/eA (
    ExpressionState:Calculating/eA (
      OperatorState:Gathering/eA (
        ValueState:Propagating/iA,
        ValueState:Propagating/iB
      )
    ),
    OperatorState:Gathering/eA (
      ValueState:Propagating/iA,
      ValueState:Propagating/iB
    )
  )
)
OperatorState:Gathering/eA[c1546d67d2322e99ddcbfdb157edb5dfb135d8aeafcdd2041290a70e130f8cfa]
ValueState:Propagating/iA[36060134d807a85bd4de45d03754fc36d6f73b2349587345f0f71b5548caeaee]
ValueState:Propagating/iB[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
`)))
		})

		It("map reusing expression", func() {
			e := db.NewExpression(NS, "C").
				AddOperand("iA", 5).
				AddOperand("iB", 6).
				AddExpressionOperation("eA", "iA+iB").
				AddExpressionOperation("eB", "eA+7")

			g := Must(controllers.GenerateGraph(log, e, NS))

			desc := bytes.NewBuffer(nil)
			MustBeSuccessful(g.Dump(desc))
			fmt.Printf("\n%s\n", desc.String())
			Expect(desc.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/eB (
  OperatorState:Exposing/eB (
    ExpressionState:Calculating/eB (
      OperatorState:Gathering/eB (
        ValueState:Propagating/eA (
          OperatorState:Exposing/eA (
            ExpressionState:Calculating/eA (
              OperatorState:Gathering/eA (
                ValueState:Propagating/iA,
                ValueState:Propagating/iB
              )
            ),
            OperatorState:Gathering/eA (
              ValueState:Propagating/iA,
              ValueState:Propagating/iB
            )
          )
        )
      )
    ),
    OperatorState:Gathering/eB (
      ValueState:Propagating/eA (
        OperatorState:Exposing/eA (
          ExpressionState:Calculating/eA (
            OperatorState:Gathering/eA (
              ValueState:Propagating/iA,
              ValueState:Propagating/iB
            )
          ),
          OperatorState:Gathering/eA (
            ValueState:Propagating/iA,
            ValueState:Propagating/iB
          )
        )
      )
    )
  )
)
OperatorState:Gathering/eA[c1546d67d2322e99ddcbfdb157edb5dfb135d8aeafcdd2041290a70e130f8cfa]
OperatorState:Gathering/eB[18c4c3be9f44a982064269c75914bde68cba853a9b7257d8461b1f6b2082b77e]
ValueState:Propagating/iA[36060134d807a85bd4de45d03754fc36d6f73b2349587345f0f71b5548caeaee]
ValueState:Propagating/iB[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
`)))
		})
	})

	It("map reusing simple op", func() {
		e := db.NewExpression(NS, "C").
			AddOperand("iA", 5).
			AddOperand("iB", 6).
			AddOperation("oA", db.OP_ADD, "iA", "7").
			AddOperation("oB", db.OP_ADD, "iA", "oA").
			AddOperation("oC", db.OP_ADD, "iA", "eA").
			AddExpressionOperation("eA", "iA+iB")

		g := Must(controllers.GenerateGraph(log, e, NS))

		desc := bytes.NewBuffer(nil)
		MustBeSuccessful(g.Dump(desc))
		fmt.Printf("\n%s\n", desc.String())
		Expect(desc.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/oC (
  OperatorState:Exposing/oC (
    ExpressionState:Calculating/oC (
      OperatorState:Gathering/oC (
        ValueState:Propagating/eA (
          OperatorState:Exposing/eA (
            ExpressionState:Calculating/eA (
              OperatorState:Gathering/eA (
                ValueState:Propagating/iA,
                ValueState:Propagating/iB
              )
            ),
            OperatorState:Gathering/eA (
              ValueState:Propagating/iA,
              ValueState:Propagating/iB
            )
          )
        )
      )
    ),
    OperatorState:Gathering/oC (
      ValueState:Propagating/eA (
        OperatorState:Exposing/eA (
          ExpressionState:Calculating/eA (
            OperatorState:Gathering/eA (
              ValueState:Propagating/iA,
              ValueState:Propagating/iB
            )
          ),
          OperatorState:Gathering/eA (
            ValueState:Propagating/iA,
            ValueState:Propagating/iB
          )
        )
      )
    )
  )
)
OperatorState:Gathering/eA[c1546d67d2322e99ddcbfdb157edb5dfb135d8aeafcdd2041290a70e130f8cfa]
ValueState:Propagating/iA[36060134d807a85bd4de45d03754fc36d6f73b2349587345f0f71b5548caeaee]
ValueState:Propagating/iB[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
OperatorState:Gathering/oC[f848afd953f2730a3092837901dcee36ad8c69b48771a7f46945cc5ef4733ca6]
`)))
	})
})
