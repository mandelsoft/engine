package controllers_test

import (
	"bytes"
	"path"

	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

var _ = Describe("Controller Test", func() {
	var env *TestEnv
	var cntr *me.ExpressionController

	_ = cntr

	var buf *bytes.Buffer
	var log logging.Logger

	BeforeEach(func() {
		buf = bytes.NewBuffer(nil)
		log = logrusl.Human().WithWriter(buf).New().Logger()
	})

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		cntr = me.NewExpressionController(env.Logging(), 1, env.Database())
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

		It("generates, evaluates(fake) and deletes external expressions", func() {
			env.Start(cntr)

			ooEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			ivA := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "A")
			ivB := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "B")
			ioE := database.NewObjectId(mymetamodel.TYPE_OPERATOR, path.Join(NS, "EXPR"), "E")
			ivE := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "E")
			ioE1 := database.NewObjectId(mymetamodel.TYPE_OPERATOR, path.Join(NS, "EXPR"), "E-1")
			ivoA := database.NewObjectId(mymetamodel.TYPE_VALUE, path.Join(NS, "EXPR"), "oA")
			eoE := env.FutureForObjectStatus(STATUS_ANY, ioE)
			eoE1 := env.FutureForObjectStatus(STATUS_ANY, ioE1)
			evoA := env.FutureForObjectStatus(STATUS_ANY, ivoA)
			eExpr := env.FutureForObjectStatus(model.STATUS_COMPLETED, ioE)
			env.SetObject(ooEXPR)

			env.WaitWithTimeout(eoE)
			env.WaitWithTimeout(eoE1)
			env.WaitWithTimeout(evoA)

			o := Must(env.GetObject(ioE))
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
			o = Must(env.GetObject(ioE1))
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

			// caclculate graph versions
			g := Must(me.GenerateGraph(log, ooEXPR, path.Join(NS, "EXPR")))

			ovA := Must(env.GetObject(ivA)).(*db.Value)
			fvA := "ValueState:Propagating/A[48208f9428d64634bd8e28ff345bf0eab60d53c18fa2fbdb0b9bc1e84df2b5f6]"
			Expect(g.FormalVersion(g.GraphIdForPhase(ivA, mymetamodel.PHASE_PROPAGATE))).To(Equal(fvA))

			ovA.Status.Status = model.STATUS_COMPLETED
			ovA.Status.FormalVersion = fvA
			MustBeSuccessful(env.SetObject(ovA))

			ovB := Must(env.GetObject(ivB)).(*db.Value)
			fvB := g.FormalVersion(g.GraphIdForPhase(ivB, mymetamodel.PHASE_PROPAGATE))
			ovB.Status.Status = model.STATUS_COMPLETED
			ovB.Status.FormalVersion = fvB
			MustBeSuccessful(env.SetObject(ovB))

			ovoA := Must(env.GetObject(ivoA)).(*db.Value)
			fvoA := g.FormalVersion(g.GraphIdForPhase(ivoA, mymetamodel.PHASE_PROPAGATE))
			ovoA.Status.Status = model.STATUS_COMPLETED
			ovoA.Status.FormalVersion = fvoA
			MustBeSuccessful(env.SetObject(ovoA))

			ooE1 := Must(env.GetObject(ioE1)).(*db.Operator)
			ooE1.AddFinalizer("test")
			foE1 := g.FormalVersion(g.GraphIdForPhase(ioE1, mymetamodel.PHASE_EXPOSE))
			doE1 := "2b2606094bcd9738aa2767157571a66de2826185db4d9c7efc03cd6a23018f34"
			ooE1.Status.Status = model.STATUS_COMPLETED
			ooE1.Status.Phase = mymetamodel.PHASE_EXPOSE
			ooE1.Status.DetectedVersion = doE1
			ooE1.Status.FormalVersion = foE1
			MustBeSuccessful(env.SetObject(ooE1))

			ovE := db.NewValueNode(ivE.GetNamespace(), ivE.GetName(), 3)
			MustBeSuccessful(env.SetObject(ovE))

			ooE := Must(env.GetObject(ioE)).(*db.Operator)
			foE := "OperatorState:Exposing/E(ExpressionState:Calculating/E(OperatorState:Gathering/E[3f02065af5c0f868b0679e5d58c89a3395b398213581d06f64d5a4c836060853](ValueState:Propagating/A[48208f9428d64634bd8e28ff345bf0eab60d53c18fa2fbdb0b9bc1e84df2b5f6],ValueState:Propagating/E-1(OperatorState:Exposing/E-1(ExpressionState:Calculating/E-1(OperatorState:Gathering/E-1[2b2606094bcd9738aa2767157571a66de2826185db4d9c7efc03cd6a23018f34](ValueState:Propagating/B[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b],ValueState:Propagating/oA[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b])),OperatorState:Gathering/E-1[2b2606094bcd9738aa2767157571a66de2826185db4d9c7efc03cd6a23018f34](ValueState:Propagating/B[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b],ValueState:Propagating/oA[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b]))))),OperatorState:Gathering/E[3f02065af5c0f868b0679e5d58c89a3395b398213581d06f64d5a4c836060853](ValueState:Propagating/A[48208f9428d64634bd8e28ff345bf0eab60d53c18fa2fbdb0b9bc1e84df2b5f6],ValueState:Propagating/E-1(OperatorState:Exposing/E-1(ExpressionState:Calculating/E-1(OperatorState:Gathering/E-1[2b2606094bcd9738aa2767157571a66de2826185db4d9c7efc03cd6a23018f34](ValueState:Propagating/B[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b],ValueState:Propagating/oA[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b])),OperatorState:Gathering/E-1[2b2606094bcd9738aa2767157571a66de2826185db4d9c7efc03cd6a23018f34](ValueState:Propagating/B[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b],ValueState:Propagating/oA[49c987621f206f09e5fbe23b516b55a36f838cb14867961f1d84a554d3a35b6b])))))"
			Expect(g.FormalVersion(g.GraphIdForPhase(ioE, mymetamodel.PHASE_EXPOSE)), foE)
			doE := "3f02065af5c0f868b0679e5d58c89a3395b398213581d06f64d5a4c836060853"
			ooE.Status.Status = model.STATUS_COMPLETED
			ooE.Status.Phase = mymetamodel.PHASE_EXPOSE
			ooE.Status.DetectedVersion = doE
			ooE.Status.FormalVersion = foE
			MustBeSuccessful(env.SetObject(ooE))

			env.WaitWithTimeout(eExpr)

			eExpr = env.FutureForObjectStatus(model.STATUS_DELETED, ooEXPR)
			eoE = env.FutureForObjectStatus(model.STATUS_DELETED, ioE)
			MustBeSuccessful(env.DeleteObject(ooEXPR))
			env.WaitWithTimeout(eoE)

			Must(db2.RemoveFinalizer(env.Database(), &ooE1, "test"))

			env.WaitWithTimeout(eExpr)
		})
	})
})
