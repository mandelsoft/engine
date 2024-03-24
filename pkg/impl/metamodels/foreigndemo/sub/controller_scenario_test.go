package sub_test

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"

	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/future"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/controllers"
	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/version"
)

var _ = Describe("Controller Scenario Test Environment", func() {
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
		It("handles the controller test scenario", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
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
			g := Must(me.GenerateGraph(log, oeEXPR, path.Join(NS, "EXPR")))

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
			Expect(ovE.(*db.Value).Status.FormalVersion).To(Equal(g.FormalVersion(g.GraphIdForPhase(ovE, mymetamodel.FINAL_VALUE_PHASE))))
		})

		It("handles expression", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			feEXPR := env.FutureForObjectStatus(model.STATUS_COMPLETED, oeEXPR)
			MustBeSuccessful(env.SetObject(oeEXPR))

			env.WaitWithTimeout(feEXPR)
			oeEXPR = Must(env.GetObject(oeEXPR)).(*db.Expression)
			Expect(oeEXPR.Status.Output).To(Equal(db.ExpressionOutput{"E": 3, "oA": 2}))
		})

		It("handles expression with constant", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+2*B")

			feEXPR := env.FutureForObjectStatus(model.STATUS_COMPLETED, oeEXPR)
			MustBeSuccessful(env.SetObject(oeEXPR))

			env.WaitWithTimeout(feEXPR)
			oeEXPR = Must(env.GetObject(oeEXPR)).(*db.Expression)
			Expect(oeEXPR.Status.Output).To(Equal(db.ExpressionOutput{"E": 6, "oA": 2}))
		})

		It("modifies expression operand for controller test scenario", func() {
			cntr := me.NewExpressionController(env.Logging(), 1, env.Database())
			env.AddService(cntr)
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			feEXPR := env.FutureForObjectStatus(model.STATUS_COMPLETED, oeEXPR)
			MustBeSuccessful(env.SetObject(oeEXPR))

			env.WaitWithTimeout(feEXPR)

			oeEXPR = Must(env.GetObject(oeEXPR)).(*db.Expression)
			Expect(oeEXPR.Status.Output).To(Equal(db.ExpressionOutput{"E": 3, "oA": 2}))

			// calculate graph versions
			//  A   = 1
			//  B   = 3
			//  oA  = A + 1      = 2
			//  E-1 = oA + B     = 5
			//  E   = E-1 - A    = 4
			logging.DefaultContext().Logger(controllers.REALM).Info("****************** modify B ******************")

			w := NewExpressionMod(env, oeEXPR, db.ExpressionOutput{"E": 4, "oA": 2})
			oeEXPR.Spec.Operands["B"] = 3
			MustBeSuccessful(env.SetObject(oeEXPR))

			Expect(w.Wait()).To(BeTrue())
		})

		It("modifies expression structure for controller test scenario", func() {
			cntr := me.NewExpressionController(env.Logging(), 1, env.Database())
			env.AddService(cntr)
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("A", 1).
				AddOperand("B", 2).
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A")

			feEXPR := env.FutureForObjectStatus(model.STATUS_COMPLETED, oeEXPR)
			MustBeSuccessful(env.SetObject(oeEXPR))

			env.WaitWithTimeout(feEXPR)

			oeEXPR = Must(env.GetObject(oeEXPR)).(*db.Expression)
			Expect(oeEXPR.Status.Output).To(Equal(db.ExpressionOutput{"E": 3, "oA": 2}))

			// calculate graph versions
			//  iA   = 1
			//  B   = 2
			//  oA  = iA + 1     = 2
			//  O-1 = oA + iA    = 3
			//  O   = O-1 + B    = 5
			logging.DefaultContext().Logger(controllers.REALM).Info("****************** modify B ******************")

			ns := NS + "/" + oeEXPR.GetName()
			w := NewExpressionMod(env, oeEXPR, db.ExpressionOutput{"O": 5, "oA": 2})
			delete(oeEXPR.Spec.Operands, "A")
			delete(oeEXPR.Spec.Expressions, "E")
			oeEXPR.Spec.Operands["iA"] = 1
			oeEXPR.AddOperand("iA", 1).
				AddOperation("oA", db.OP_ADD, "iA", "1").
				AddExpressionOperation("O", "oA+iA+B")
			foE1 := env.FutureForObjectStatus(model.STATUS_DELETED, database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "E-1"))
			foE := env.FutureForObjectStatus(model.STATUS_DELETED, database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "E"))
			MustBeSuccessful(env.SetObject(oeEXPR))

			Expect(w.Wait()).To(BeTrue())

			Expect(env.WaitWithTimeout(foE)).To(BeTrue())
			Expect(env.WaitWithTimeout(foE1)).To(BeTrue())

			ExpectError(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "A"))).To(HaveOccurred())
			ExpectError(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "E"))).To(HaveOccurred())
			ExpectError(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "E-1"))).To(HaveOccurred())
			ExpectError(env.GetObject(database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "E"))).To(HaveOccurred())
			ExpectError(env.GetObject(database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "E-1"))).To(HaveOccurred())
		})

		It("deletes expression for controller test scenario", func() {
			cntr := me.NewExpressionController(env.Logging(), 1, env.Database())
			env.AddService(cntr)
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

			feEXPR := env.FutureForObjectStatus(model.STATUS_COMPLETED, ooEXPR)
			env.SetObject(ooEXPR)

			env.WaitWithTimeout(feEXPR)

			ooEXPR = Must(env.GetObject(ooEXPR)).(*db.Expression)
			Expect(ooEXPR.Status.Output).To(Equal(db.ExpressionOutput{"E": 3, "oA": 2}))

			// delete the enchilada
			feEXPR = env.FutureForObjectStatus(model.STATUS_DELETED, ooEXPR)
			MustBeSuccessful(env.DeleteObject(ooEXPR))
			Expect(env.WaitWithTimeout(feEXPR)).To(BeTrue())

			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_VALUE, true, path.Join(NS, "EXPR")))).To(BeEmpty())
			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_OPERATOR, true, path.Join(NS, "EXPR")))).To(BeEmpty())
			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_EXPRESSION, true, path.Join(NS, "EXPR")))).To(BeEmpty())
			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_VALUE_STATE, true, path.Join(NS, "EXPR")))).To(BeEmpty())
			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_OPERATOR_STATE, true, path.Join(NS, "EXPR")))).To(BeEmpty())
			Expect(Must(env.Database().ListObjectIds(mymetamodel.TYPE_EXPRESSION_STATE, true, path.Join(NS, "EXPR")))).To(BeEmpty())

			Expect(cntr.GetTriggers()).To(BeEmpty())
		})

		It("handles operator for controller test scenario", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			ovA := db.NewValueNode(NS, "A", 1)
			ovB := db.NewValueNode(NS, "B", 2)

			ooEXPR := db.NewOperatorNode(NS, "EXPR").
				AddOperand("A", "A").
				AddOperand("B", "B").
				AddOperation("oA", db.OP_ADD, "A", "1").
				AddExpressionOperation("E", "oA+B-A").
				AddOutput("O", "E")

			MustBeSuccessful(env.SetObject(ovA))
			MustBeSuccessful(env.SetObject(ovB))

			// caclculate graph versions
			//  A   = 1
			//  B   = 2
			//  oA  = 2
			//  E   = E-1 - A    = 3
			//  E-1 = oA + B     = 4

			ivO := database.NewObjectId(mymetamodel.TYPE_VALUE, NS, "O")
			fvO := env.FutureForObjectStatus(model.STATUS_COMPLETED, ivO)

			MustBeSuccessful(env.SetObject(ooEXPR))

			env.WaitWithTimeout(fvO)

			ovO := Must(env.GetObject(ivO)).(*db.Value)
			Expect(ovO.Spec.Value).To(Equal(3))
		})
	})

	Context("modification", func() {
		It("modifies", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			oeEXPR := db.NewExpression(NS, "EXPR").
				AddOperand("iA", 5).
				AddOperand("iB", 6).
				AddOperation("oA", db.OP_ADD, "iA", "iB").
				AddExpressionOperation("oE", "iA+oA+iB")

			// caclculate graph versions
			//  iA   = 5
			//  iB   = 6
			//  oA   = 11
			//  oE   = E-1 - iB  = 10
			//  oE-1 = iA + oA   = 16

			// subns := oeEXPR.GetNamespace() + "/" + oeEXPR.GetName()
			ivoE1 := version.NewId(mmids.NewTypeId(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE), "oE-1")
			ivoE := version.NewId(mmids.NewTypeId(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE), "oE")
			g := Must(me.GenerateGraph(log, oeEXPR, path.Join(NS, "EXPR")))
			g.Dump(os.Stdout)

			fmt.Printf("\nobjects:\n")
			for _, o := range g.Objects() {
				fmt.Printf("- %s\n", o)
			}
			fmt.Printf("\nroot objects:\n")
			for _, o := range g.RootObjects() {
				fmt.Printf("- %s\n", o)
			}
			fmt.Printf("\ncheck objects:\n")
			for _, o := range g.CheckObjects() {
				fmt.Printf("- %s\n", o)
			}

			fmt.Printf("formal oE-1: %s\n", g.FormalVersion(ivoE1))
			fmt.Printf("formal oE:   %s\n", g.FormalVersion(ivoE))

			ns := NS + "/" + "EXPR"
			genobjs := []database.ObjectId{
				database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "iA"),
				database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "iB"),
				database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "oA"),
				database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "oE"),
				database.NewObjectId(mymetamodel.TYPE_OPERATOR, ns, "oE-1"),
			}
			rootobjs := []database.ObjectId{
				database.NewObjectId(mymetamodel.TYPE_VALUE, ns, "oE"),
			}
			chkobjs := append(genobjs, rootobjs...)
			Expect(g.Objects()).To(ConsistOf(genobjs))
			Expect(g.RootObjects()).To(ConsistOf(rootobjs))
			Expect(g.CheckObjects()).To(ConsistOf(chkobjs))
		})
	})
})

type ExpressionMon struct {
	oid    database.ObjectId
	f      future.Future
	env    *TestEnv
	output db.ExpressionOutput
}

func NewExpressionMod(env *TestEnv, oid database.ObjectId, output db.ExpressionOutput) *ExpressionMon {
	f := env.FutureForObjectStatus(model.STATUS_COMPLETED, oid, true)

	return &ExpressionMon{
		oid:    database.NewObjectIdFor(oid),
		f:      f,
		env:    env,
		output: output,
	}
}

func (e ExpressionMon) Wait() bool {
	t := time.Now().Add(10 * time.Second)
	for {
		if !e.env.WaitWithTimeout(e.f) {
			return false
		}
		o := Must(e.env.GetObject(e.oid)).(*db.Expression)
		if reflect.DeepEqual(o.Status.Output, e.output) {
			return true
		}
		now := time.Now()
		if now.After(t) {
			fmt.Printf("foind output of %s: %#v\n", e.oid, o.Status.Output)
			return false
		}
	}
}
