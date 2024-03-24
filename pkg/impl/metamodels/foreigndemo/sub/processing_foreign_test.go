package sub_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/goutils/general"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	"github.com/mandelsoft/engine/pkg/version"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
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

		It("operator with constant", func() {
			env.AddService(controllers.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))

			opC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "2").
				AddOperation("eA", db.OP_ADD, "iA", "iB").
				AddOutput("C-A", "eA")

			fvA := graph.NewValue(vA)
			fopC := graph.NewOperator(opC)

			g := Must(graph.NewGraph(version.Composed, fvA, fopC))

			rid := database.NewObjectId(mymetamodel.TYPE_VALUE, NS, "C-A")
			expected := g.FormalVersion(g.MapToPhaseId(rid))

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 7, "C")

			o := Must(env.GetObject(rid))
			fmt.Printf("\n%s\n", expected)
			fmt.Printf("%s\n", o.(*db.Value).Status.FormalVersion)
			Expect(o.(*db.Value).Status.FormalVersion).To(Equal(expected))

			fmt.Println("************** deleting operator ****************")
			fuOpC := env.FutureForObjectStatus(model.STATUS_DELETED, opC)
			MustBeSuccessful(env.DeleteObject(opC))
			Expect(env.WaitWithTimeout(fuOpC)).To(BeTrue())
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

			fvA := graph.NewValue(vA)
			fvB := graph.NewValue(vB)
			fopC := graph.NewOperator(opC)

			g := Must(graph.NewGraph(version.Composed, fvA, fvB, fopC))

			rid := database.NewObjectId(mymetamodel.TYPE_VALUE, NS, "C-A")
			expected := g.FormalVersion(g.MapToPhaseId(rid))

			mCA := ValueCompleted(env, "C-A")
			MustBeSuccessful(env.SetObject(opC))

			Expect(env.Wait(mCA)).To(BeTrue())
			mCA.Check(env, 11, "C")

			o := Must(env.GetObject(rid))
			fmt.Printf("\n%s\n", expected)
			fmt.Printf("%s\n", o.(*db.Value).Status.FormalVersion)
			Expect(o.(*db.Value).Status.FormalVersion).To(Equal(expected))

			fmt.Println("************** deleting operator ****************")
			fuOpC := env.FutureForObjectStatus(model.STATUS_DELETED, opC)
			MustBeSuccessful(env.DeleteObject(opC))
			Expect(env.WaitWithTimeout(fuOpC)).To(BeTrue())
		})
	})

	Context("failures", func() {
		It("reports invalid", func() {
			env.AddService(controllers.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(env.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(env.SetObject(vB))

			vopC := db.NewOperatorNode(NS, "C").
				AddOperand("iA", "A").
				AddOperand("iB", "B").
				AddOperation("eA", "noop", "iA", "iB").
				AddOutput("C-A", "eA")

			fopC := env.FutureFor(model.STATUS_INVALID, NewElementIdForTypePhase(mymetamodel.TYPE_OPERATOR_STATE, vopC, mymetamodel.PHASE_GATHER))
			MustBeSuccessful(env.SetObject(vopC))

			Expect(env.WaitWithTimeout(fopC)).To(BeTrue())
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
	max := general.Optional(omax...)
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
	if provider != "" && v.(*db.Value).Status.Provider != provider {
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
