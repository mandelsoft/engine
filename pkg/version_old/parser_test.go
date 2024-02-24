package version_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-test/deep"

	me "github.com/mandelsoft/engine/pkg/version"
)

var _ = Describe("parser", func() {
	It("simple version", func() {
		n := me.NewNode(T, "A", "v1")
		Expect(me.GetId(n)).To(Equal("node/A:node/A[v1]"))

		g, err := me.Parse(me.GetId(n))

		Expect(err).To(Succeed())

		Expect(deep.Equal(n, g)).To(BeNil())
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
		Expect(me.GetId(n1)).To(Equal("node/A(node/B(node/D(node/E)),node/C(node/D)):node/A[v1],node/B[v2],node/C[v3],node/D[v4],node/E[v5]"))

		g, err := me.Parse(me.GetId(n1))
		Expect(err).To(Succeed())

		Expect(deep.Equal(n1, g)).To(BeNil())
	})
})
