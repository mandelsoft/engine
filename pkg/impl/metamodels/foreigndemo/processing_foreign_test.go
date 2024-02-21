package foreigndemo_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/controllers"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	"github.com/mandelsoft/engine/pkg/utils"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/db"
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
			MustBeSuccessfull(env.SetObject(n5))

			Expect(env.Wait(mn5)).To(BeTrue())

			mn5.Check(env, 5, "")
		})

		It("reached waiting state for expression", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mEx := env.FutureFor(model.STATUS_WAITING, NewElementId(mymetamodel.TYPE_EXPRESSION_STATE, NS, "C", mymetamodel.PHASE_EVALUATION))
			MustBeSuccessfull(env.SetObject(opC))

			Expect(env.Wait(mEx)).To(BeTrue())
		})

		It("operator with two operands (in order)", func() {
			env.AddService(controllers.NewExpressionController(env.Context(), env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessfull(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")
		})

		It("recalculates operator with two operands (in order)", func() {
			env.AddService(controllers.NewExpressionController(env.Context(), env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			mCA := ValueCompleted(env, "C-A", true)
			MustBeSuccessfull(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")

			fmt.Printf("*** modify value A ***\n")
			_ = Must(database.Modify(env.Database(), &vA, func(o *db.Value) (bool, bool) {
				support.UpdateField(&o.Spec.Value, utils.Pointer(4))
				return true, true
			}))

			Expect(mCA.WaitUntil(env, 10, "C", 3)).To(BeTrue())
		})
	})

	Context("deletion", func() {
		var fCA processor.Future
		var vA *db.Value
		var vB *db.Value
		var opC *db.Operator

		BeforeEach(func() {
			env.AddService(controllers.NewExpressionController(env.Context(), env.Logging(), 1, env.Database()))
			env.Start()

			vA = db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB = db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))

			opC = db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			fCA = env.FutureFor(model.STATUS_COMPLETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "C-A", mymetamodel.FINAL_VALUE_PHASE), true)
			MustBeSuccessfull(env.SetObject(opC))
		})

		It("deletes all", func() {
			Expect(env.WaitWithTimeout(fCA)).To(BeTrue())
			fmt.Printf("*********************************** deleting ************************************\n")

			fvA := env.FutureFor(model.STATUS_DELETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "A", mymetamodel.FINAL_VALUE_PHASE))
			fvB := env.FutureFor(model.STATUS_DELETED, NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "B", mymetamodel.FINAL_VALUE_PHASE))

			MustBeSuccessfull(env.DéleteObject(vA))
			MustBeSuccessfull(env.DéleteObject(vB))
			MustBeSuccessfull(env.DéleteObject(opC))

			Expect(env.WaitWithTimeout(fvA)).To(BeTrue())
			Expect(env.WaitWithTimeout(fvB)).To(BeTrue())

			Expect(env.List(mymetamodel.TYPE_EXPRESSION_STATE, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_VALUE_STATE, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_OPERATOR_STATE, NS)).To(BeNil())

			Expect(env.List(mymetamodel.TYPE_EXPRESSION, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_VALUE, NS)).To(BeNil())
			Expect(env.List(mymetamodel.TYPE_OPERATOR, NS)).To(BeNil())
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
	odb := objectbase.GetDatabase[db2.DBObject](env.Processor().Model().ObjectBase())
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
	odb := objectbase.GetDatabase[db2.DBObject](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
