package sub_test

import (
	"bytes"
	"fmt"
	"path"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"

	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

var _ = Describe("Test Environment", func() {
	var env *TestEnv
	var buf *bytes.Buffer
	var log logging.Logger

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		buf = bytes.NewBuffer(nil)
		log = logrusl.Human().WithWriter(buf).New().Logger()
	})

	AfterEach(func() {
		if env != nil {
			env.Cleanup()
		}
	})

	Context("", func() {
		FIt("handles the controller test scenario", func() {
			env.AddService(me.NewExpressionController(env.Context(), env.Logging(), 1, env.Database()))
			env.Start()

			ooEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			// caclculate graph versions
			//  A   = 1
			//  B   = 2
			//  oA  = 2
			//  E   = E-1 - A    = 3
			//  E-1 = oA + B     = 4
			g := Must(me.GenerateGraph(log, ooEXPR, path.Join(NS, "EXPR")))

			ivE := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "E")

			fvE := env.FutureForObjectStatus(model.STATUS_COMPLETED, ivE)
			for _, id := range g.Objects() {
				fmt.Printf("creating %s\n", id)
				o := g.GetObject(id)
				MustBeSuccessful(env.SetObject(o.(db2.Object)))
			}
			Expect(env.WaitWithTimeout(fvE)).To(BeTrue())

			ovE := Must(env.GetObject(ivE))

			Expect(ovE.(*db.Value).Spec.Value).To(Equal(3))
			Expect(ovE.(*db.Value).Status.FormalVersion).To(Equal(g.FormalVersion(graph.GraphIdForPhase(ovE, mymetamodel.FINAL_VALUE_PHASE))))
		})
	})
})
