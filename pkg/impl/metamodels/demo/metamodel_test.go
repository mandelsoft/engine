package demo_test

import (
	"bytes"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo"
)

var _ = Describe("meta model", func() {
	It("validate", func() {

		spec := demo.NewModelSpecification("test")
		Expect(spec.Validate()).To(Succeed())
	})

	It("evaluates", func() {
		spec := demo.NewModelSpecification("test")

		buf := &bytes.Buffer{}
		m := Must(spec.GetMetaModel())
		m.Dump(buf)

		Expect("\n" + buf.String()).To(Equal(`
Namespace type: Namespace
External types:
- Node  (-> NodeState:Updating)
  internal type: NodeState
  phase:         Updating
Internal types:
- NodeState
  phases:
  - Updating
Element types:
- NodeState:Updating
  dependencies:
  - NodeState:Updating
`))
	})
})
