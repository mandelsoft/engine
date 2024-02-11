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
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/processor"

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

func (m *ValueMon) Check(env *TestEnv, value int, provider string) {
	odb := objectbase.GetDatabase[support.DBObject](env.Processor().Model().ObjectBase())
	v, err := odb.GetObject(m.oid)
	ExpectWithOffset(1, err).To(Succeed())
	ExpectWithOffset(1, v.(*db.Value).Status.Provider).To(Equal(provider))
	ExpectWithOffset(1, v.(*db.Value).Spec.Value).To(Equal(value))
}
