package sub_test

import (
	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	. "github.com/mandelsoft/engine/pkg/processing/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	"github.com/mandelsoft/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/processing/model"

	mymodel "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
)

var _ = Describe("Controller Scenario Test Environment", func() {
	var env *TestEnv
	var log logging.Logger

	BeforeEach(func() {
		env = Must(NewTestEnv("test", "testdata", mymodel.NewModelSpecification))
		log = logging.DefaultContext().Logger(logging.Realm("engine"))
	})

	AfterEach(func() {
		if env != nil {
			env.Cleanup()
		}
	})

	Context("", func() {
		It("handles the controller test scenario", func() {
			env.AddService(me.NewExpressionController(env.Logging(), 1, env.Database()))
			env.Start()

			ovA := db.NewValueNode(NS, "A", 5)
			ovB := db.NewValueNode(NS, "B", 6)

			MustBeSuccessful(env.SetObject(ovA))
			MustBeSuccessful(env.SetObject(ovB))

			orA := db.NewUpdateRequest(NS, "A").RequestAction(model.REQ_ACTION_ACQUIRE)
			frA := env.FutureForObjectStatus(model.Status(model.REQ_STATUS_ACQUIRED), orA)
			log.Info("STEP 1: create update request")
			MustBeSuccessful(env.SetObject(orA))
			Expect(env.WaitWithTimeout(frA)).To(BeTrue())

			frA = env.FutureForObjectStatus(model.Status(model.REQ_STATUS_LOCKED), orA)
			log.Info("STEP 2: request locks")
			MustBeSuccessful(Modify(env, &orA, func(o *db.UpdateRequest) (bool, bool) {
				o.SetAction(&model.UpdateAction{
					Action: model.REQ_ACTION_LOCK,
					Objects: []database.LocalObjectRef{
						database.NewLocalObjectRefFor(ovA),
						database.NewLocalObjectRefFor(ovB),
					},
				})
				return true, true
			}))
			Expect(env.WaitWithTimeout(frA)).To(BeTrue())
			owner := processor.Owner(orA.GetName())

			o := Must(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, "A")))
			Expect(processor.IsObjectLock(o.(*db.ValueState).RunId)).To(Equal(&owner))
			o = Must(env.GetObject(database.NewObjectId(mymetamodel.TYPE_VALUE_STATE, NS, "B")))
			Expect(processor.IsObjectLock(o.(*db.ValueState).RunId)).To(Equal(&owner))

			log.Info("STEP 3: update value")
			MustBeSuccessful(Modify(env, &ovA, func(o *db.Value) (bool, bool) {
				o.Spec.Value = 8
				return true, true
			}))
			MustBeSuccessful(Modify(env, &ovB, func(o *db.Value) (bool, bool) {
				o.Spec.Value = 9
				return true, true
			}))

			mvA := NewValueMon(env, model.STATUS_COMPLETED, ovA.GetName())
			mvB := NewValueMon(env, model.STATUS_COMPLETED, ovB.GetName())
			frA = env.FutureForObjectStatus(model.Status(model.REQ_STATUS_RELEASED), orA)
			log.Info("STEP 4:release request")
			MustBeSuccessful(Modify(env, &orA, func(o *db.UpdateRequest) (bool, bool) {
				o.SetAction(&model.UpdateAction{
					Action:  model.REQ_ACTION_RELEASE,
					Objects: o.Spec.Objects,
				})
				return true, true
			}))
			Expect(env.WaitWithTimeout(frA)).To(BeTrue())

			log.Info("STEP 5: waiting for new valuest")
			Expect(mvA.WaitUntil(env, 8, "")).To(BeTrue())
			Expect(mvB.WaitUntil(env, 9, "")).To(BeTrue())
		})
	})
})
