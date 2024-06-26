package multidemo_test

import (
	"bytes"
	"fmt"

	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
)

var _ = Describe("meta model", func() {
	It("validate", func() {

		spec := me.NewModelSpecification("test", nil)
		Expect(spec.Validate()).To(Succeed())
	})

	It("initializes", func() {

		spec := me.NewModelSpecification("test", nil)
		types := spec.Objectbase.SchemeTypes()

		o := Must(types.CreateObject(mymetamodel.TYPE_NODE, objectbase.SetObjectName("namespace", "test")))

		Expect(o.GetName()).To(Equal("test"))
		Expect(o.GetNamespace()).To(Equal("namespace"))
		Expect(o.GetType()).To(Equal(mymetamodel.TYPE_NODE))

	})

	It("evaluates", func() {
		spec := me.NewModelSpecification("test", nil)

		buf := &bytes.Buffer{}
		m := Must(spec.GetMetaModel())
		m.Dump(buf)

		fmt.Printf("\n%s\n", buf.String())
		Expect("\n" + buf.String()).To(Equal(`
Namespace type: Namespace
External types:
- Node  (-> NodeState:Gathering)
  internal type: NodeState
  phase:         Gathering
Internal types:
- NodeState
  phases:
  - Calculating
  - Gathering
  trigger types:
  - Node
Element types:
- NodeState:Calculating
  dependencies:
  - NodeState:Gathering (local)
  updated states:
  - Node
- NodeState:Gathering
  triggered by: Node
  dependencies:
  - NodeState:Calculating
  updated states:
  - Node
`))
	})
})
