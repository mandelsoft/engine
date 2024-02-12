package foreigndemo_test

import (
	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/db"
	"github.com/mandelsoft/engine/pkg/processing/model"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
)

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

	Context("", func() {
		FIt("", func() {
			env.Start(cntr)

			vEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("E", db.OP_ADD, "A", "B")

			completed := env.FutureForObjectStatus(model.STATUS_COMPLETED, vEXPR)
			env.SetObject(vEXPR)

			env.WaitWithTimeout(completed)
		})
	})
})
