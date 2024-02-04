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
		It("plain value", func() {
			proc.Start(wg)

			n5 := db.NewValueNode(NS, "A", 5)
			n5completed := proc.CompletedFuture(common.NewElementId(mymetamodel.TYPE_VALUE_STATE, NS, "A", mymetamodel.FINAL_VALUE_PHASE))

			MustBeSuccessfull(odb.SetObject(n5))

			Expect(n5completed.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())

			n5n := Must(odb.GetObject(n5))

			Expect(n5n.(*db.Value).Status.Provider).To(Equal(""))
		})

		FIt("operator with two operands (in order) creating value node", func() {
			proc.Start(wg)

			vA := db.NewValueNode(NS, "A", 5)
			MustBeSuccessfull(odb.SetObject(vA))
			vB := db.NewValueNode(NS, "B", 6)
			MustBeSuccessfull(odb.SetObject(vB))
			opC := db.NewOperatorNode(NS, "C", "A", "B").AddOperation(db.OP_ADD, "C-A")

			vopCAid := common.NewObjectId(mymetamodel.TYPE_VALUE, NS, "C-A")
			vopCAcompleted := proc.CompletedFuture(common.NewElementIdForPhase(common.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, "C-A"), mymetamodel.FINAL_VALUE_PHASE))
			MustBeSuccessfull(odb.SetObject(opC))

			Expect(vopCAcompleted.Wait(ctxutil.WatchdogContext(ctx, 20*time.Second))).To(BeTrue())
			vopCA := Must(odb.GetObject(vopCAid))

			Expect(vopCA.(*db.Value).Status.Provider).To(Equal("C"))
			Expect(vopCA.(*db.Value).Spec.Value).To(Equal(11))
		})
	})
})
