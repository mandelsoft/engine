package multidemo_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"time"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"

	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
	"github.com/mandelsoft/vfs/pkg/vfs"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/processor"
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

	Context("regular", func() {
		It("single node", func() {
			proc.Start(ctx)

			n5 := db.NewValueNode(NS, "A", 5)
			n5completed := proc.FutureFor(model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "A", mymetamodel.FINAL_PHASE))

			MustBeSuccessful(odb.SetObject(n5))

			Expect(n5completed.Wait(ctxutil.TimeoutContext(ctx, 20*time.Second))).To(BeTrue())

			n5n := Must(odb.GetObject(n5))

			Expect(n5n.(*db.Value).Status.Result).NotTo(BeNil())
			Expect(*n5n.(*db.Value).Status.Result).To(Equal(5))
		})

		It("node with two operands (in order)", func() {
			proc.Start(ctx)

			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(odb.SetObject(n6))
			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			nacompleted := proc.FutureFor(model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "C", mymetamodel.FINAL_PHASE))
			MustBeSuccessful(odb.SetObject(na))

			Expect(nacompleted.Wait(ctxutil.TimeoutContext(ctx, 20*time.Second))).To(BeTrue())
			nan := Must(odb.GetObject(na))

			Expect(nan.(*db.Operator).Status.Result).NotTo(BeNil())
			Expect(*nan.(*db.Operator).Status.Result).To(Equal(11))
		})

		It("node with two operands (wrong order)", func() {
			lctx.Logger().Info("starting {{path}}", "path", "testdata", "other", "some value")
			lctx.Logger().Debug("debug logs enabled")
			// os.Stdout.Write(logbuf.Bytes())

			proc.Start(ctx)

			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			nacompleted := proc.FutureFor(model.STATUS_COMPLETED, mmids.NewElementId(mymetamodel.TYPE_NODE_STATE, NS, "C", mymetamodel.FINAL_PHASE), true)
			MustBeSuccessful(odb.SetObject(na))
			runtime.Gosched()
			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessful(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessful(odb.SetObject(n6))

			var result *int
			for i := 0; i < 3; i++ {
				fmt.Printf("snyc %d\n", i+1)
				Expect(nacompleted.Wait(ctxutil.TimeoutContext(ctx, 20*time.Second))).To(BeTrue())
				fmt.Printf("found completed\n")
				n := Must(odb.GetObject(na))
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
			dbo := (db2.Object)(n5)
			_ = Must(database.Modify(odb, &dbo, func(o db2.Object) (bool, bool) {
				o.(*db.Value).Spec.Value = generics.Pointer(6)
				return true, true
			}))

			Expect(nacompleted.Wait(ctxutil.TimeoutContext(ctx, 20*time.Second))).To(BeTrue())

			n := Must(odb.GetObject(na))
			result = n.(*db.Operator).Status.Result
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(12))
		})
	})
})
