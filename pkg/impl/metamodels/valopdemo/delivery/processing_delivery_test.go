package delivery_test

import (
	"bytes"
	"context"
	"sync"
	"time"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
	"github.com/mandelsoft/vfs/pkg/vfs"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

const NS = "testspace"

var _ = Describe("Processing", func() {
	var wg *sync.WaitGroup
	var fs vfs.FileSystem
	// var ob objectbase.Objectbase
	var ctx context.Context
	var lctx logging.Context
	var logbuf *bytes.Buffer
	var proc *processing.Processor
	var odb database.Database[support.DBObject]

	BeforeEach(func() {
		fs = Must(TestFileSystem("testdata", false))

		spec := mymodel.NewModelSpecification("test", filesystem.NewSpecification[support.DBObject]("testdata", fs))
		MustBeSuccessfull(spec.Validate())

		logbuf = bytes.NewBuffer(nil)
		logcfg := logrusl.Human(true)
		// logcfg=logcfg.WithWriter(logbuf)
		logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

		lctx = logging.DefaultContext()
		lctx.AddRule(logging.NewConditionRule(logging.TraceLevel, logging.NewRealmPrefix("engine/processor")))

		ctx = ctxutil.CancelContext(context.Background())

		m := Must(model.NewModel(spec))
		proc = Must(processing.NewProcessor(ctx, lctx, m, 1))
		odb = objectbase.GetDatabase[support.DBObject](proc.Model().ObjectBase())
		wg = &sync.WaitGroup{}
		_ = logbuf
	})

	AfterEach(func() {
		ctxutil.Cancel(ctx)
		wg.Wait()
		vfs.Cleanup(fs)
	})

	Context("", func() {
		It("plain value", func() {
			proc.Start(wg)

			n5 := db.NewValueNode(NS, "A", 5)
			n5completed := proc.CompletedFuture(common.NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "A", mymetamodel.FINAL_VALUE_PHASE))

			MustBeSuccessfull(odb.SetObject(n5))

			Expect(n5completed.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())

			n5n := Must(odb.GetObject(n5))

			Expect(n5n.(*db.Value).Status.Provider).To(Equal(""))
		})

		It("operator with two operands (in order) creating value node", func() {
			proc.Start(wg)

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(odb.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(odb.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", "A", "B").AddOperation(db.OP_ADD, "C-A")

			mCA := NewValueMon(proc, "C-A")
			MustBeSuccessfull(odb.SetObject(opC))

			Expect(mCA.Wait(ctx)).To(BeTrue())
			mCA.Check(proc, 11, "C")
		})

		It("multiple operators with multiple outputs creating value node", func() {
			proc.Start(wg)

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(odb.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(odb.SetObject(vB))
			vD := db.NewValueNode(NS, "D", 7)
			MustBeSuccessfull(odb.SetObject(vD))

			opC := db.NewOperatorNode(NS, "C", "B", "A").
				AddOperation(db.OP_ADD, "C-A").
				AddOperation(db.OP_SUB, "C-S")

			opE := db.NewOperatorNode(NS, "E", "D", "C-A").
				AddOperation(db.OP_MUL, "E-A")

			mEA := NewValueMon(proc, "E-A")
			mCS := NewValueMon(proc, "C-S")

			MustBeSuccessfull(odb.SetObject(opC))
			MustBeSuccessfull(odb.SetObject(opE))

			Expect(mEA.Wait(ctx)).To(BeTrue())
			Expect(mCS.Wait(ctx)).To(BeTrue())

			mCS.Check(proc, 1, "C")
			mEA.Check(proc, 77, "E")

		})

		It("multiple operators with multiple outputs creating value node (wrong order)", func() {
			proc.Start(wg)

			vA := db.NewValueNode(NS, "A", 5)
			vB := db.NewValueNode(NS, "B", 6)
			vD := db.NewValueNode(NS, "D", 7)

			opC := db.NewOperatorNode(NS, "C", "B", "A").
				AddOperation(db.OP_ADD, "C-A").
				AddOperation(db.OP_SUB, "C-S")

			opE := db.NewOperatorNode(NS, "E", "D", "C-A").
				AddOperation(db.OP_MUL, "E-A")

			mEA := NewValueMon(proc, "E-A")
			mCS := NewValueMon(proc, "C-S")

			MustBeSuccessfull(odb.SetObject(opE))
			MustBeSuccessfull(odb.SetObject(opC))

			MustBeSuccessfull(odb.SetObject(vD))
			MustBeSuccessfull(odb.SetObject(vB))
			MustBeSuccessfull(odb.SetObject(vA))

			Expect(mEA.Wait(ctx)).To(BeTrue())
			Expect(mCS.Wait(ctx)).To(BeTrue())

			mCS.Check(proc, 1, "C")
			mEA.Check(proc, 77, "E")

		})
	})
})

type ValueMon struct {
	oid       model.ObjectId
	sid       model.ElementId
	completed processing.Future
}

func NewValueMon(proc *processing.Processor, name string, retrigger ...bool) *ValueMon {
	oid := common.NewObjectId(mymetamodel.TYPE_VALUE, NS, name)
	sid := common.NewElementIdForPhase(common.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, name), mymetamodel.FINAL_VALUE_PHASE)

	return &ValueMon{
		oid:       oid,
		sid:       sid,
		completed: proc.CompletedFuture(sid, retrigger...),
	}
}

func (m *ValueMon) Wait(ctx context.Context) bool {
	return m.completed.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))
}

func (m *ValueMon) Check(proc *processing.Processor, value int, provider string) {
	odb := objectbase.GetDatabase[support.DBObject](proc.Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
