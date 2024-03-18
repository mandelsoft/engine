package expression_test

import (
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/expression"
	. "github.com/mandelsoft/engine/pkg/testutils"
	"github.com/mandelsoft/engine/pkg/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Expression Parsing", func() {
	Context("leafs", func() {
		It("value", func() {
			Expect(expression.Parse("12")).To(Equal(&expression.Node{Value: utils.Pointer(12)}))
		})
		It("negative value", func() {
			Expect(expression.Parse("---12")).To(Equal(&expression.Node{Value: utils.Pointer(-12)}))
		})
		It("name", func() {
			Expect(expression.Parse("varA")).To(Equal(&expression.Node{Name: "varA"}))
		})
		It("name with digits", func() {
			Expect(expression.Parse("var4A5")).To(Equal(&expression.Node{Name: "var4A5"}))
		})
	})

	Context("expressions", func() {
		It("add", func() {
			Expect(Must(expression.Parse("A+1")).String()).To(Equal("(A+1)"))
		})
		It("sub", func() {
			Expect(Must(expression.Parse("A-1")).String()).To(Equal("(A-1)"))
		})
		It("mul", func() {
			Expect(Must(expression.Parse("A*1")).String()).To(Equal("(A*1)"))
		})
		It("div", func() {
			Expect(Must(expression.Parse("A/1")).String()).To(Equal("(A/1)"))
		})

		It("chained", func() {
			Expect(Must(expression.Parse("A+B+C")).String()).To(Equal("((A+B)+C)"))
		})
		It("order", func() {
			Expect(Must(expression.Parse("A+B*C+D")).String()).To(Equal("((A+(B*C))+D)"))
		})
		It("complex", func() {
			Expect(Must(expression.Parse("A+B*(C+D)+1")).String()).To(Equal("((A+(B*(C+D)))+1)"))
		})

		It("blanks", func() {
			Expect(Must(expression.Parse(" A + B * ( C + D ) + 1 ")).String()).To(Equal("((A+(B*(C+D)))+1)"))
		})
	})

	Context("operands", func() {
		n := Must(expression.Parse("A+B*(C+D)+1"))
		Expect(n.Operands()).To(Equal([]string{"A", "B", "C", "D"}))
	})
})
