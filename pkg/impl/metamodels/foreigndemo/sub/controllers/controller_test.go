package controllers_test

import (
	"path"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/processing/model"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
)

var _ = Describe("Controller Test", func() {
	var env *TestEnv
	var cntr *me.ExpressionController

	_ = cntr

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		cntr = me.NewExpressionController(env.Context(), env.Logging(), 1, env.Database())
	})

	AfterEach(func() {
		if env != nil {
			env.Cleanup()
		}
	})

	Context("expression", func() {
		It("completes", func() {
			env.Start(cntr)

			vEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("E", db.OP_ADD, "A", "B")

			completed := env.FutureForObjectStatus(model.STATUS_COMPLETED, vEXPR)
			env.SetObject(vEXPR)

			env.WaitWithTimeout(completed)
			o := Must(env.GetObject(vEXPR))
			Expect(o.(*db.Expression).Status.Output).To(Equal(db.ExpressionOutput{"E": 3}))
		})

		It("fails", func() {
			env.Start(cntr)

			vEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("E", db.OP_ADD, "A", "C")

			failed := env.FutureForObjectStatus(model.STATUS_FAILED, vEXPR)
			env.SetObject(vEXPR)

			env.WaitWithTimeout(failed)
			o := Must(env.GetObject(vEXPR))
			Expect(o.(*db.Expression).Status.Message).To(Equal("operand \"C\" for expression \"E\" not found"))
		})

		It("fails for complex expression", func() {
			env.Start(cntr)

			vEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddExpressionOperation("E", "(A+1)*B+C")

			failed := env.FutureForObjectStatus(model.STATUS_FAILED, vEXPR)
			env.SetObject(vEXPR)

			env.WaitWithTimeout(failed)
			o := Must(env.GetObject(vEXPR))
			Expect(o.(*db.Expression).Status.Message).To(Equal("operand \"C\" for expression \"E\" not found"))
		})

		It("generates external expressions", func() {
			env.Start(cntr)

			vEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			iE := database.NewObjectId(mymetamodel.TYPE_OPERATOR, path.Join(NS, "EXPR"), "E")
			iE1 := database.NewObjectId(mymetamodel.TYPE_OPERATOR, path.Join(NS, "EXPR"), "E-1")
			ioA := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "oA")
			eE := env.FutureForObjectStatus(STATUS_ANY, iE)
			eE1 := env.FutureForObjectStatus(STATUS_ANY, iE1)
			eoA := env.FutureForObjectStatus(STATUS_ANY, ioA)
			env.SetObject(vEXPR)

			env.WaitWithTimeout(eE)
			env.WaitWithTimeout(eE1)
			env.WaitWithTimeout(eoA)

			o := Must(env.GetObject(iE))
			Expect(o.(*db.Operator).Spec).To(YAMLEqual(`
  operands:
    O1: E-1
    O2: A
  operations:
    E:
      operands:
      - O1
      - O2
      operator: sub
  outputs:
    E: E
`))
			o = Must(env.GetObject(iE1))
			Expect(o.(*db.Operator).Spec).To(YAMLEqual(`
  operands:
    O1: oA
    O2: B
  operations:
    E:
      operands:
      - O1
      - O2
      operator: add
  outputs:
    E-1: E
`))
		})
	})
})
