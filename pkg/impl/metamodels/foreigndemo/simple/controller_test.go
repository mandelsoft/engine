package simple_test

import (
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/db"
	"github.com/mandelsoft/engine/pkg/processing/model"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
)

var _ = Describe("Controller Test", func() {
	var env *TestEnv
	var cntr *controllers.ExpressionController

	_ = cntr

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		cntr = controllers.NewExpressionController(env.Logging(), 1, env.Database())
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
	})
})
