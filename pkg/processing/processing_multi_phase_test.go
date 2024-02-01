package processing_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
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
	demo "github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	mm "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/utils"
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

		spec := demo.NewModelSpecification("test", filesystem.NewSpecification[support.DBObject]("testdata", fs))
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
		odb = objectbase.GetDatabase[support.DBObject](proc.Objectbase())
		wg = &sync.WaitGroup{}
		_ = logbuf
	})

	AfterEach(func() {
		ctxutil.Cancel(ctx)
		wg.Wait()
		vfs.Cleanup(fs)
	})

	Context("", func() {
		It("single node", func() {
			proc.Start(wg)

			n5 := db.NewValueNode(NS, "A", 5)
			n5completed := proc.CompletedFuture(common.NewElementId(mm.TYPE_NODE_STATE, NS, "A", mm.FINAL_PHASE))

			MustBeSuccessfull(odb.SetObject(n5))

			Expect(n5completed.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())

			n5n := Must(odb.GetObject(n5))

			Expect(n5n.(*db.Node).Status.Result).NotTo(BeNil())
			Expect(*n5n.(*db.Node).Status.Result).To(Equal(5))
		})

		It("node with two operands (in order)", func() {
			proc.Start(wg)

			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(odb.SetObject(n6))
			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			nacompleted := proc.CompletedFuture(common.NewElementId(mm.TYPE_NODE_STATE, NS, "C", mm.FINAL_PHASE))
			MustBeSuccessfull(odb.SetObject(na))

			Expect(nacompleted.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())
			nan := Must(odb.GetObject(na))

			Expect(nan.(*db.Node).Status.Result).NotTo(BeNil())
			Expect(*nan.(*db.Node).Status.Result).To(Equal(11))
		})

		FIt("node with two operands (wrong order)", func() {
			lctx.Logger().Info("starting {{path}}", "path", "testdata", "other", "some value")
			lctx.Logger().Debug("debug logs enabled")
			// os.Stdout.Write(logbuf.Bytes())

			proc.Start(wg)

			na := db.NewOperatorNode(NS, "C", db.OP_ADD, "A", "B")
			nacompleted := proc.CompletedFuture(common.NewElementId(mm.TYPE_NODE_STATE, NS, "C", mm.FINAL_PHASE), true)
			MustBeSuccessfull(odb.SetObject(na))
			runtime.Gosched()
			n5 := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(odb.SetObject(n5))
			n6 := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(odb.SetObject(n6))

			var result *int
			for i := 0; i < 3; i++ {
				fmt.Printf("snyc %d\n", i+1)
				Expect(nacompleted.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())
				fmt.Printf("found completed\n")
				n := Must(odb.GetObject(na))
				result = n.(*db.Node).Status.Result
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
			_ = Must(database.Modify(odb, &dbo, func(o support.DBObject) (bool, bool) {
				o.(*db.Node).Spec.Value = utils.Pointer(6)
				return true, true
			}))

			Expect(nacompleted.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())

			n := Must(odb.GetObject(na))
			result = n.(*db.Node).Status.Result
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(12))
		})
	})
})