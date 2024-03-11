package simple_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	"github.com/mandelsoft/engine/pkg/utils"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

const NS = "testspace"

var _ = Describe("Processing", func() {
	var env *TestEnv

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
	})

	AfterEach(func() {
		if env != nil {
			env.Cleanup()
		}
	})

	Context("", func() {
		It("plain value", func() {
			env.Start()

			n5 := db.NewValueNode(NS, "A", 5)
			mn5 := ValueCompleted(env, "A")
			MustBeSuccessful(env.SetObject(n5))

			Expect(env.Wait(mn5)).To(BeTrue())

			mn5.Check(env, 5, "")
		})

		It("reached waiting state for expression", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mEx := env.FutureFor(model.STATUS_WAITING, NewElementId(mymetamodel.TYPE_EXPRESSION_STATE, NS, "C", mymetamodel.PHASE_CALCULATE))
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mEx)).To(BeTrue())
		})

		It("operator with two operands (in order)", func() {
			env.AddService(controllers.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")
		})

		It("recalculates operator with two operands (in order)", func() {
			env.AddService(controllers.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mCA := ValueCompleted(env, "C-A", true)
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")

			fmt.Printf("*** modify value A ***\n")
			_ = Must(database.Modify(env.Database(), &vA, func(o *db.Value) (bool, bool) {
				support.UpdateField(&o.Spec.Value, utils.Pointer(4))
				return true, true
			}))

			Expect(mCA.WaitUntil(env, 10, "C", 3)).To(BeTrue())

			o := Must(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE, NS, "C-A")))
			data := Must(yaml.Marshal(o))
			fmt.Printf("result:\n%s\n", string(data))
			Expect(o.(*db.Value).Status.FormalVersion).To(Equal(
				"ValueState:Propagating/C-A(OperatorState:Exposing/C(ExpressionState:Calculating/C(OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef](ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012])),OperatorState:Gathering/C[6f646e6ab9bd10e0fc3eeec777ded31ffa70af3f832ebc5ad68a303781c42fef](ValueState:Propagating/A[07953a67895cdbe07665002609a1c24dc503557aadb8db223e398fd2e7593132],ValueState:Propagating/B[10e7d612060343a8046dfaef0bb9ee50a1d25dc67bc370468a787e47ff0f0012])))",
			))
		})
	})

	Context("deletion", func() {
		var fCA processor.Future
		var vA *db.Value
		var vB *db.Value
		var opC *db.Operator

		BeforeEach(func() {
			env.AddService(controllers.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			vA = db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB = db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))

			opC = db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			fCA = env.FutureFor(model.STATUS_COMPLETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "C-A", mymetamodel.FINAL_VALUE_PHASE), true)
			MustBeSuccessful(env.SetObject(opC))
		})

		It("deletes all", func() {
			Expect(env.WaitWithTimeout(fCA)).To(BeTrue())
			fmt.Printf("*********************************** deleting ************************************\n")

			fvA := env.FutureFor(model.STATUS_DELETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "A", mymetamodel.FINAL_VALUE_PHASE))
			fvB := env.FutureFor(model.STATUS_DELETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "B", mymetamodel.FINAL_VALUE_PHASE))

			MustBeSuccessful(env.DeleteObject(vA))
			MustBeSuccessful(env.DeleteObject(vB))
			MustBeSuccessful(env.DeleteObject(opC))

			Expect(env.WaitWithTimeout(fvA)).To(BeTrue())
			Expect(env.WaitWithTimeout(fvB)).To(BeTrue())

			Expect(env.List(mymetamodel.TYPE_EXPRESSION_STATE, true, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_VALUE_STATE, true, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_OPERATOR_STATE, true, NS)).To(BeNil())

			Expect(env.List(mymetamodel.TYPE_EXPRESSION, true, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_VALUE, true, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_OPERATOR, true, NS)).To(BeNil())
		})
	})
})

type ValueMon struct {
	etype  processor.EventType
	oid    ObjectId
	sid    ElementId
	future processor.Future
}

func NewValueMon(env *TestEnv, etype processor.EventType, name string, retrigger ...bool) *ValueMon {
	oid := mmids.NewObjectId(mymetamodel.TYPE_VALUE, NS, name)
	sid := mmids.NewElementIdForPhase(mmids.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, name), mymetamodel.FINAL_VALUE_PHASE)

	return &ValueMon{
		etype:  etype,
		oid:    oid,
		sid:    sid,
		future: env.FutureFor(etype, sid, retrigger...),
	}
}

func ValueCompleted(env *TestEnv, name string, retrigger ...bool) *ValueMon {
	return NewValueMon(env, model.STATUS_COMPLETED, name, retrigger...)
}

func ValueDeleted(env *TestEnv, name string, retrigger ...bool) *ValueMon {
	return NewValueMon(env, model.STATUS_DELETED, name, retrigger...)
}

func (m *ValueMon) ObjectId() database.ObjectId {
	return m.oid
}

func (m *ValueMon) ElementId() ElementId {
	return m.sid
}

func (m *ValueMon) StateObjectId() database.ObjectId {
	return m.sid.ObjectId()
}

func (m *ValueMon) Wait(ctx context.Context) bool {
	ctx = ctxutil.TimeoutContext(ctx, 20*time.Second)
	b := m.future.Wait(ctx)
	if b {
		fmt.Printf("FOUND %s %s\n", m.sid, m.etype)
	} else {
		fmt.Printf("ABORTED %s %s\n", m.sid, m.etype)
	}
	ctxutil.Cancel(ctx)
	return b
}

func (m *ValueMon) WaitUntil(env *TestEnv, value int, provider string, omax ...int) bool {
	max := utils.Optional(omax...)
	// max==0 means endless
	for {
		max--
		if max == 0 {
			return false
		}
		if !m.Wait(env.Context()) {
			return false
		}
		if m.Test(env, value, provider) {
			return true
		}
	}
}

func (m *ValueMon) Test(env *TestEnv, value int, provider string) bool {
	odb := objectbase.GetDatabase[db2.Object](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	if v.(*db.Value).Status.Provider != provider {
		return false
	}
	if v.(*db.Value).Spec.Value != value {
		return false
	}
	return true
}

func (m *ValueMon) Check(env *TestEnv, value int, provider string) {
	odb := objectbase.GetDatabase[db2.Object](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
