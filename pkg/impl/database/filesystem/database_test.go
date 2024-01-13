package filesystem_test

import (
	"github.com/go-test/deep"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/vfs/pkg/vfs"

	me "github.com/mandelsoft/engine/pkg/impl/database/filesystem"
)

var _ = Describe("database", func() {
	var db database.Database
	var fs vfs.FileSystem

	BeforeEach(func() {
		fs = Must(TestFileSystem("testdata", false))
		db = Must(me.New(Scheme, "testdata", fs))
	})

	Context("list", func() {
		It("flat ns", func() {
			list := Must(db.ListObjects(TYPE_A, "ns1"))
			Expect(list).To(ConsistOf(NewA("ns1", "o1", "A-ns1-o1")))
		})

		It("deep ns", func() {
			list := Must(db.ListObjects(TYPE_B, "ns11"))
			Expect(list).To(BeEmpty())

			list = Must(db.ListObjects(TYPE_B, "ns1/sub1"))
			Expect(list).To(ConsistOf(NewB("ns1/sub1", "o1", "B-ns1/sub1-o1")))
		})
		It("all", func() {
			list := Must(db.ListObjects(TYPE_B, ""))
			Expect(list).To(ConsistOf(
				NewB("ns1/sub1", "o1", "B-ns1/sub1-o1"),
				NewB("ns2", "o2", "B-ns2-o2"),
			))
		})
	})

	Context("write", func() {
		It("writes object", func() {
			a := NewA("ns3/sub1", "o2", "A-ns3/sub1-o2")
			MustBeSuccessfull(db.SetObject(a))

			Expect(deep.Equal(Must(db.GetObject(a)), a)).To(BeNil())
			list := Must(db.ListObjects(TYPE_A, "ns3/sub1"))
			Expect(list).To(ConsistOf(
				a,
			))

		})
	})
})
