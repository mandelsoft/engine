package explicit_test

import (
	"context"
	"fmt"
	"runtime"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/processor"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
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
		It("single node", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			mvA := NewValueMon(env, "A")

			MustBeSuccessfull(env.SetObject(vA))

			Expect(env.Wait(mvA)).To(BeTrue())
			mvA.Check(env, 5)
		})

		It("node with two operands (in order)", func() {
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")

			opCcompleted := env.CompletedFuture(mmids.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, NS, "C", mymetamodel.FINAL_OPERATOR_PHASE))
			MustBeSuccessfull(env.SetObject(opC))

			Expect(opCcompleted.Wait(ctxutil.TimeoutContext(env.Context(), 20*time.Second))).To(BeTrue())
			nan := Must(env.GetObject(opC))

			Expect(nan.(*db.Operator).Status.Result).NotTo(BeNil())
			Expect(*nan.(*db.Operator).Status.Result).To(Equal(11))
		})

		It("node with two operands (wrong order)", func() {
			env.Start()

			nr := db.NewResultNode(NS, "D", "C")
			MustBeSuccessfull(env.SetObject(nr))

			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			nacompleted := env.CompletedFuture(mmids.NewElementId(mymetamodel.TYPE_OPERATOR_STATE, NS, "C", mymetamodel.FINAL_OPERATOR_PHASE), true)
			MustBeSuccessfull(env.SetObject(na))
			runtime.Gosched()
			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(env.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(env.SetObject(n6))

			var result *int
			for i := 0; i < 3; i++ {
				fmt.Printf("snyc %d\n", i+1)
				Expect(nacompleted.Wait(ctxutil.TimeoutContext(env.Context(), 20*time.Second))).To(BeTrue())
				fmt.Printf("found completed\n")
				n := Must(env.GetObject(na))
				result = n.(*db.Operator).Status.Result
				if result != nil {
					if *result == 11 {
						fmt.Printf("found result %d\n", *result)
						break
					} else {
						fmt.Printf("found result %d, but expected 11\n", *result)
					}
				} else {
					fmt.Printf("found no result\n")
				}
			}

			fmt.Printf("*** modify object A ***\n")
			dbo := (support.DBObject)(n5)
			_ = Must(database.Modify(env.Database(), &dbo, func(o support.DBObject) (bool, bool) {
				o.(*db.Value).Spec.Value = 6
				return true, true
			}))

			Expect(nacompleted.Wait(ctxutil.TimeoutContext(env.Context(), 20*time.Second))).To(BeTrue())

			n := Must(env.GetObject(na))
			result = n.(*db.Operator).Status.Result
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(12))

			nrcompleted := env.CompletedFuture(mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "D", mymetamodel.FINAL_VALUE_PHASE))
			Expect(nrcompleted.Wait(ctxutil.TimeoutContext(env.Context(), 20*time.Second))).To(BeTrue())

			n = Must(env.GetObject(nr))
			result = n.(*db.Value).Status.Result
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(12))
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
	b := m.completed.Wait(ctxutil.TimeoutContext(ctx, 20*time.Second))
	if b {
		fmt.Printf("FOUND %s completed\n", m.sid)
	} else {
		fmt.Printf("ABORTED %s\n", m.sid)
	}
	return b
}

func (m *ValueMon) Check(env *TestEnv, value int) {
	odb := objectbase.GetDatabase[support.DBObject](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Result).NotTo(BeNil())
	ExpectWithOffset(1, *v.(*db.Value).Status.Result).To(Equal(value))
}
