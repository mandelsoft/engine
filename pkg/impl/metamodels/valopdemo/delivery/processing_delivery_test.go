package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/processor"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
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

		It("operator with two operands (in order) creating value node", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", "A", "B").AddOperation(db.OP_ADD, "C-A")

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")
		})

		It("multiple operators with multiple outputs creating value node", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))
			vD := db.NewValueNode(NS, "D", 7)
			MustBeSuccessful(env.SetObject(vD))

			opC := db.NewOperatorNode(NS, "C", "B", "A").
				AddOperation(db.OP_ADD, "C-A").
				AddOperation(db.OP_SUB, "C-S")

			opE := db.NewOperatorNode(NS, "E", "D", "C-A").
				AddOperation(db.OP_MUL, "E-A")

			mEA := ValueCompleted(env, "E-A")
			mCS := ValueCompleted(env, "C-S")

			MustBeSuccessful(env.SetObject(opC))
			MustBeSuccessful(env.SetObject(opE))

			Expect(env.Wait(mEA)).To(BeTrue())
			Expect(env.Wait(mCS)).To(BeTrue())

			mCS.Check(env, 1, "C")
			mEA.Check(env, 77, "E")
		})

		It("multiple operators with multiple outputs creating value node (wrong order)", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			vB := db.NewValueNode(NS, "B", 6)
			vD := db.NewValueNode(NS, "D", 7)

			opC := db.NewOperatorNode(NS, "C", "B", "A").
				AddOperation(db.OP_ADD, "C-A").
				AddOperation(db.OP_SUB, "C-S")

			opE := db.NewOperatorNode(NS, "E", "D", "C-A").
				AddOperation(db.OP_MUL, "E-A")

			mEA := ValueCompleted(env, "E-A")
			mCS := ValueCompleted(env, "C-S")

			MustBeSuccessful(env.SetObject(opE))
			MustBeSuccessful(env.SetObject(opC))

			MustBeSuccessful(env.SetObject(vD))
			MustBeSuccessful(env.SetObject(vB))
			MustBeSuccessful(env.SetObject(vA))

			Expect(env.Wait(mEA)).To(BeTrue())
			Expect(env.Wait(mCS)).To(BeTrue())

			mCS.Check(env, 1, "C")
			mEA.Check(env, 77, "E")
		})
	})

	////////////////////////////////////////////////////////////////////////////

	Context("changing structure", func() {
		It("operator with multiple outputs", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", "A", "B").AddOperation(db.OP_ADD, "C-A")

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")

			fmt.Printf("*** modify operator C ***\n")
			mCB := ValueCompleted(env, "C-B")
			mCA = ValueDeleted(env, "C-A")
			_ = Must(database.Modify(env.Database(), &opC, func(o *db.Operator) (bool, bool) {
				o.Spec.Operations = []db.Operation{
					db.Operation{
						Operator: db.OP_MUL,
						Target:   "C-B",
					},
				}
				return true, true
			}))

			Expect(env.Wait(mCB)).To(BeTrue())
			mCB.Check(env, 30, "C")
			Expect(env.Wait(mCA)).To(BeTrue())

			_, err := env.GetObject(mCA.ObjectId())
			Expect(errors.Is(err, database.ErrNotExist)).To(BeTrue())
			o, err := env.GetObject(mCA.StateObjectId())
			Expect(errors.Is(err, database.ErrNotExist)).To(BeTrue())

			time.Sleep(time.Second)
			_ = o
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
	oid := NewObjectId(mymetamodel.TYPE_VALUE, NS, name)
	sid := NewElementIdForPhase(NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, name), mymetamodel.FINAL_VALUE_PHASE)

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

func (m *ValueMon) Check(env *TestEnv, value int, provider string) {
	odb := objectbase.GetDatabase[db2.Object](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
