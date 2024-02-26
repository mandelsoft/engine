package version_test

import (
	"bytes"
	"fmt"
	"strings"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/version"
)

var _ = Describe("Test Environment", func() {
	Context("", func() {
		It("", func() {
			v := "ValueState:Propagating/C-A(OperatorState:Calculating/C(ExpressionState:Evaluating/C(OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef](ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012])),OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef](ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012])))"

			n := Must(me.Parse(v))
			buf := bytes.NewBuffer(nil)

			MustBeSuccessfull(n.Dump(buf))
			fmt.Printf("\n%s\n", buf.String())
			Expect(buf.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/C-A (
  OperatorState:Calculating/C (
    ExpressionState:Evaluating/C (
      OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef] (
        ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],
        ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
      )
    ),
    OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef] (
      ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],
      ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
    )
  )
)
`)))
			g := Must(n.AsGraph())

			buf.Reset()
			MustBeSuccessfull(g.Dump(buf))
			fmt.Printf("\n%s\n", buf.String())
			Expect(buf.String()).To(Equal(strings.TrimSpace(`
ValueState:Propagating/C-A (
  OperatorState:Calculating/C (
    ExpressionState:Evaluating/C (
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
OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef]
ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132]
ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012]
`)))
		})
	})
})
