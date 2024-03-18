package wrapper_test

import (
	"context"
	"sync"

	. "github.com/mandelsoft/engine/pkg/database/wrapper/testtypes"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-test/deep"

	"github.com/mandelsoft/engine/pkg/database"
	me "github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/database/wrapper/support"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	dbtypes "github.com/mandelsoft/engine/pkg/impl/database/filesystem/testtypes"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

var _ = Describe("wrapper", func() {
	var backend database.Database[dbtypes.Object]
	var db me.Database[Object, Object, dbtypes.Object]

	var reg database.HandlerRegistrationTest
	var fs vfs.FileSystem

	BeforeEach(func() {
		fs = Must(TestFileSystem("testdata", false))
		backend = Must(filesystem.New[dbtypes.Object](dbtypes.Scheme, "testdata", fs))
	})

	AfterEach(func() {
		vfs.Cleanup(fs)
	})

	Context("identity mapping", func() {
		BeforeEach(func() {
			db = Must(me.NewDatabase[Object, Object, dbtypes.Object](backend, Scheme.(database.SchemeTypes[Object]), support.IdentityMapping[dbtypes.Object]{})) // Goland
			reg = db.(database.HandlerRegistrationTest)
		})

		Context("list", func() {
			It("flat ns", func() {
				list := Must(db.ListObjects(TYPE_A, false, "ns1"))
				Expect(list).To(ConsistOf(NewA("ns1", "o1", "A-ns1-o1")))
			})

			It("deep ns", func() {
				list := Must(db.ListObjects(TYPE_B, false, "ns11"))
				Expect(list).To(BeEmpty())

				list = Must(db.ListObjects(TYPE_B, false, "ns1/sub1"))
				Expect(list).To(ConsistOf(NewB("ns1/sub1", "o1", "B-ns1/sub1-o1")))
			})

			It("all", func() {
				list := Must(db.ListObjects(TYPE_B, true, ""))
				Expect(list).To(ConsistOf(
					NewB("ns1/sub1", "o1", "B-ns1/sub1-o1"),
					NewB("ns2", "o2", "B-ns2-o2"),
				))
			})
		})

		Context("write", func() {
			It("writes object", func() {
				a := NewA("ns3/sub1", "o2", "A-ns3/sub1-o2")
				MustBeSuccessful(db.SetObject(a))

				Expect(deep.Equal(Must(db.GetObject(a)), a)).To(BeNil())
				list := Must(db.ListObjects(TYPE_A, false, "ns3/sub1"))
				Expect(list).To(ConsistOf(
					a,
				))

			})
		})

		Context("event handler", func() {
			It("gets events for all objects", func() {
				h := &Handler{}
				db.RegisterHandler(h, true, TYPE_A, true, "").Wait(context.Background())
				Expect(h.ids).To(ConsistOf(
					database.NewObjectId(TYPE_A, "ns1", "o1"),
					database.NewObjectId(TYPE_A, "ns2", "o1"),
				))
			})

			It("gets events for all actual objects before new ones", func() {
				notify := make(chan struct{})

				h := &Handler{}
				s := reg.RegisterHandlerSync(notify, h, true, TYPE_A, true, "")
				err := db.SetObject(NewA("ns3/sub1", "o2", "A-ns3/sub1-o2"))
				notify <- struct{}{}
				Expect(err).To(Succeed())

				s.Wait(context.Background())

				Expect(h.ids).To(ConsistOf(
					database.NewObjectId(TYPE_A, "ns1", "o1"),
					database.NewObjectId(TYPE_A, "ns2", "o1"),
					database.NewObjectId(TYPE_A, "ns3/sub1", "o2"),
				))
			})
		})

		Context("race condition detection", func() {
			It("increments generation", func() {
				id := database.NewObjectId(TYPE_A, "ns1", "o1")
				o1 := Must(db.GetObject(id))
				Expect(database.GetGeneration(o1)).To(Equal(int64(0)))

				o1.(*A).SetData("modified")
				MustBeSuccessful(db.SetObject(o1))
				Expect(database.GetGeneration(o1)).To(Equal(int64(1)))

				o1 = Must(db.GetObject(id))
				Expect(database.GetGeneration(o1)).To(Equal(int64(1)))
				Expect(o1.(*A).GetData()).To(Equal("modified"))
			})

			It("detects race condition", func() {
				id := database.NewObjectId(TYPE_A, "ns1", "o1")
				o1 := Must(db.GetObject(id))
				o2 := Must(db.GetObject(id))
				Expect(database.GetGeneration(o1)).To(Equal(int64(0)))
				Expect(database.GetGeneration(o2)).To(Equal(int64(0)))

				o1.(*A).SetData("modified")
				o2.(*A).SetData("first")

				MustBeSuccessful(db.SetObject(o2))
				Expect(database.GetGeneration(o2)).To(Equal(int64(1)))

				Expect(db.SetObject(o1)).To(MatchError("object modified"))

				o1 = Must(db.GetObject(id))
				Expect(database.GetGeneration(o1)).To(Equal(int64(1)))
				Expect(o1.(*A).GetData()).To(Equal("first"))
			})
		})
	})

	////////////////////////////////////////////////////////////////////////////
})

type Handler struct {
	lock sync.Mutex
	ids  []database.ObjectId
}

var _ database.EventHandler = (*Handler)(nil)

func (h *Handler) HandleEvent(id database.ObjectId) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.ids = append(h.ids, id)
}
