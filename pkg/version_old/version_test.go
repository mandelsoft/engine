package version_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/version_old"
)

const T = "node"

var _ = Describe("version", func() {
	It("simple version", func() {
		n := me.NewNode(T, "A", "v1")
		Expect(me.GetId(n)).To(Equal("node/A:node/A[v1]"))
	})

	It("simple dep version", func() {
		n1 := me.NewNode(T, "A", "v1")
		n2 := me.NewNode(T, "B", "v2")
		n1.AddDep(n2)
		Expect(me.GetId(n1)).To(Equal("node/A(node/B):node/A[v1],node/B[v2]"))
	})

	It("simple deep dep version", func() {
		n1 := me.NewNode(T, "A", "v1")
		n2 := me.NewNode(T, "B", "v2")
		n3 := me.NewNode(T, "C", "v3")
		n4 := me.NewNode(T, "D", "v4")
		n1.AddDep(n2)
		n1.AddDep(n3)
		n3.AddDep(n4)
		fmt.Printf("%s\n", me.GetId(n1))
		Expect(me.GetId(n1)).To(Equal("node/A(node/B,node/C(node/D)):node/A[v1],node/B[v2],node/C[v3],node/D[v4]"))
	})

	It("route", func() {
		n1 := me.NewNode(T, "A", "v1")
		n2 := me.NewNode(T, "B", "v2")
		n3 := me.NewNode(T, "C", "v3")
		n4 := me.NewNode(T, "D", "v4")
		n1.AddDep(n2)
		n1.AddDep(n3)
		n2.AddDep(n4)
		n3.AddDep(n4)
		fmt.Printf("%s\n", me.GetId(n1))
		Expect(me.GetId(n1)).To(Equal("node/A(node/B(node/D),node/C(node/D)):node/A[v1],node/B[v2],node/C[v3],node/D[v4]"))
	})

	It("shatred", func() {
		n1 := me.NewNode(T, "A", "v1")
		n2 := me.NewNode(T, "B", "v2")
		n3 := me.NewNode(T, "C", "v3")
		n4 := me.NewNode(T, "D", "v4")
		n5 := me.NewNode(T, "E", "v5")
		n1.AddDep(n2)
		n1.AddDep(n3)
		n2.AddDep(n4)
		n3.AddDep(n4)
		n4.AddDep(n5)
		fmt.Printf("%s\n", me.GetId(n1))
		vers := "node/A(node/B(node/D(node/E)),node/C(node/D)):node/A[v1],node/B[v2],node/C[v3],node/D[v4],node/E[v5]"
		Expect(me.GetId(n1)).To(Equal(vers))

		v := me.NewNodeVersion(n1)
		Expect(v.GetId()).To(Equal(vers))
		Expect(v.GetHash()).To(Equal("7f5e8cbca1eecf8efd4d190ef96a3d3e424c634fb276c156e60bde8168cda785"))
	})

})
