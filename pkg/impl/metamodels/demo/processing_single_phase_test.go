package demo_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"time"

	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
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
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	"github.com/mandelsoft/engine/pkg/utils"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/demo"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

const NS = "testspace"

var _ = Describe("Processing", func() {
	var fs vfs.FileSystem
	// var ob objectbase.Objectbase
	var ctx context.Context
	var lctx logging.Context
	var logbuf *bytes.Buffer
	var proc *processor.Processor
	var odb database.Database[db2.Object]

	BeforeEach(func() {
		fs = Must(TestFileSystem("testdata", false))

		spec := mymodel.NewModelSpecification("test", filesystem.NewSpecification[db2.Object]("testdata", fs))
		MustBeSuccessful(spec.Validate())

		logbuf = bytes.NewBuffer(nil)
		logcfg := logrusl.Human(true)
		// logcfg=logcfg.WithWriter(logbuf)
		logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

		lctx = logging.DefaultContext()
		lctx.AddRule(logging.NewConditionRule(logging.TraceLevel, logging.NewRealmPrefix("engine/processor")))

		ctx = ctxutil.CancelContext(context.Background())

		m := Must(model.NewModel(spec))
		proc = Must(processor.NewProcessor(lctx, m, 1))
		odb = objectbase.GetDatabase[db2.Object](proc.Model().ObjectBase())
		_ = logbuf
	})

	AfterEach(func() {
		ctxutil.Cancel(ctx)
		proc.Wait()
		vfs.Cleanup(fs)
	})

	Context("", func() {
		It("single node", func() {
			proc.Start(ctx)

			n5 := db.NewValueNode(NS, "A", 5)

			f := proc.FutureFor(model.STATUS_COMPLETED, model.NewElementIdForType(mymetamodel.TYPE_NODE_STATE, n5, mymetamodel.FINAL_PHASE))
			MustBeSuccessful(odb.SetObject(n5))

			f.Wait(ctxutil.TimeoutContext(ctx, 10*time.Second))
			// Expect(proc.Wait(ctxutil.TimeoutContext(ctx, 10*time.Second))).To(BeTrue())

			n5n := Must(odb.GetObject(n5))

			Expect(n5n.(*db.Node).Status.Result).NotTo(BeNil())
			Expect(*n5n.(*db.Node).Status.Result).To(Equal(5))
		})

		It("node with two operands (in order)", func() {
			proc.Start(ctx)

			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(odb.SetObject(n6))
			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			MustBeSuccessful(odb.SetObject(na))

			Expect(proc.WaitFor(ctxutil.TimeoutContext(ctx, 20*time.Second), model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "C", mymetamodel.FINAL_PHASE))).To(BeTrue())

			nan := Must(odb.GetObject(na))

			Expect(nan.(*db.Node).Status.Result).NotTo(BeNil())
			Expect(*nan.(*db.Node).Status.Result).To(Equal(11))
		})

		It("node with two operands (wrong order)", func() {
			lctx.Logger().Info("starting {{path}}", "path", "testdata", "other", "some value")
			lctx.Logger().Debug("debug logs enabled")
			// os.Stdout.Write(logbuf.Bytes())

			proc.Start(ctx)

			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			MustBeSuccessful(odb.SetObject(na))
			runtime.Gosched()
			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(odb.SetObject(n6))

			var result *int
			for i := 0; i < 3; i++ {
				fmt.Printf("snyc %d\n", i+1)
				Expect(proc.WaitFor(ctxutil.TimeoutContext(ctx, 20*time.Second), model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "C", mymetamodel.FINAL_PHASE))).To(BeTrue())
				n := Must(odb.GetObject(na))
				result = n.(*db.Node).Status.Result
				if result != nil && *result == 11 {
					break
				}
			}

			fmt.Printf("*** modify object A ***\n")
			dbo := (db2.Object)(n5)
			_ = Must(database.Modify(odb, &dbo, func(o db2.Object) (bool, bool) {
				o.(*db.Node).Spec.Value = utils.Pointer(6)
				return true, true
			}))

			Expect(proc.WaitFor(ctxutil.TimeoutContext(ctx, 20*time.Second), model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "C", mymetamodel.FINAL_PHASE))).To(BeTrue())

			n := Must(odb.GetObject(na))
			result = n.(*db.Node).Status.Result
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(12))
		})
	})
})
