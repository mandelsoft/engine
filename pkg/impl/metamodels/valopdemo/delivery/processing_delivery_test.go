package delivery_test

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
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
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
			mn5 := NewValueMon(env, "A")
			MustBeSuccessfull(env.SetObject(n5))

			Expect(env.Wait(mn5)).To(BeTrue())

			mn5.Check(env, 5, "")
		})

		It("operator with two operands (in order) creating value node", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", "A", "B").AddOperation(db.OP_ADD, "C-A")

			mCA := NewValueMon(env, "C-A")
			MustBeSuccessfull(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")
		})

		It("multiple operators with multiple outputs creating value node", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))
			vD := db.NewValueNode(NS, "D", 7)
			MustBeSuccessfull(env.SetObject(vD))

			opC := db.NewOperatorNode(NS, "C", "B", "A").
				AddOperation(db.OP_ADD, "C-A").
				AddOperation(db.OP_SUB, "C-S")

			opE := db.NewOperatorNode(NS, "E", "D", "C-A").
				AddOperation(db.OP_MUL, "E-A")

			mEA := NewValueMon(env, "E-A")
			mCS := NewValueMon(env, "C-S")

			MustBeSuccessfull(env.SetObject(opC))
			MustBeSuccessfull(env.SetObject(opE))

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

			mEA := NewValueMon(env, "E-A")
			mCS := NewValueMon(env, "C-S")

			MustBeSuccessfull(env.SetObject(opE))
			MustBeSuccessfull(env.SetObject(opC))

			MustBeSuccessfull(env.SetObject(vD))
			MustBeSuccessfull(env.SetObject(vB))
			MustBeSuccessfull(env.SetObject(vA))

			Expect(env.Wait(mEA)).To(BeTrue())
			Expect(env.Wait(mCS)).To(BeTrue())

			mCS.Check(env, 1, "C")
			mEA.Check(env, 77, "E")

		})
	})
})

type ValueMon struct {
	oid       ObjectId
	sid       ElementId
	completed processor.Future
}

func NewValueMon(env *TestEnv, name string, retrigger ...bool) *ValueMon {
	oid := mmids.NewObjectId(mymetamodel.TYPE_VALUE, NS, name)
	sid := mmids.NewElementIdForPhase(mmids.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, name), mymetamodel.FINAL_VALUE_PHASE)

	return &ValueMon{
		oid:       oid,
		sid:       sid,
		completed: env.CompletedFuture(sid, retrigger...),
	}
}

func (m *ValueMon) Wait(ctx context.Context) bool {
	b := m.completed.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))
	if b {
		fmt.Printf("FOUND %s completed\n", m.sid)
	} else {
		fmt.Printf("ABORTED %s\n", m.sid)
	}
	return b
}

func (m *ValueMon) Check(env *TestEnv, value int, provider string) {
	odb := objectbase.GetDatabase[support.DBObject](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
