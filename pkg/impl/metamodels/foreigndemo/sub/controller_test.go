package sub_test

import (
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/processing/model"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
)

const NS = "testspace"

var _ = Describe("Controller Test", func() {
	var env *TestEnv
	var cntr *controllers.ExpressionController

	_ = cntr

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		cntr = controllers.NewExpressionController(env.Context(), env.Logging(), 1, env.Database())
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
	})
})
