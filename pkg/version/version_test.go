package version_test

import (
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/version"
)

const T = "node"

var _ = Describe("version", func() {
	var graph me.Graph

	BeforeEach(func() {
		graph = me.NewGraph()
	})

	Context("composed", func() {
		It("simple version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1"))

			e := Must(me.EvaluateGraph(graph))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("node/A[v1]"))
		})

		It("tree version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2"))
			graph.AddNode(me.NewNode(T, "C", "v3"))

			e := Must(me.EvaluateGraph(graph))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("node/A[v1](node/B[v2],node/C[v3])"))
		})
		It("diamond version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "C", "v3", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "D", "v4"))

			e := Must(me.EvaluateGraph(graph))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("node/A[v1](node/B[v2](node/D[v4]),node/C[v3](node/D[v4]))"))
		})
	})

	Context("hashed", func() {
		It("simple version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1"))

			e := Must(me.EvaluateGraph(graph, me.Hashed))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("884927f20f84406c325e351af89f0925bbeddbbeeab002a75bf01819fb3705b1"))
		})

		It("tree version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2"))
			graph.AddNode(me.NewNode(T, "C", "v3"))

			e := Must(me.EvaluateGraph(graph, me.Hashed))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("9d334876abc0046311327f8bbce8ebfa33ff40ed9dda5db99081e8ade693a0a1"))
		})
		It("diamond version", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "C", "v3", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "D", "v4"))

			e := Must(me.EvaluateGraph(graph, me.Hashed))
			Expect(e.FormalVersion(me.NewId(T, "A"))).To(Equal("2a6e718480e698e18a97e19752a6969095c45754d88d28732990f1e10c92be93"))
		})
	})

	Context("failures", func() {

		It("handles missing node", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "C", "v3", me.NewId(T, "D")))

			_, err := me.EvaluateGraph(graph)
			Expect(err).To(MatchError("unknown node \"node/D\" used in \"node/B\""))
		})

		It("handles detects cycle", func() {
			graph.AddNode(me.NewNode(T, "A", "v1", me.NewId(T, "C"), me.NewId(T, "B")))
			graph.AddNode(me.NewNode(T, "B", "v2", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "C", "v3", me.NewId(T, "D")))
			graph.AddNode(me.NewNode(T, "D", "v3", me.NewId(T, "B")))

			_, err := me.EvaluateGraph(graph)
			Expect(err).To(MatchError("dependency cycle node/B->node/D->node/B"))
		})
	})

})
