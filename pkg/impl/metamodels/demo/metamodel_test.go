package demo_test

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/impl/metamodels/demo"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

var _ = Describe("meta model", func() {
	It("marshal nil", func() {
		d := Must(json.Marshal(nil))
		fmt.Printf("%s\n", string(d))
	})

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
  trigger types:
  - Node
Element types:
- NodeState:Updating
  dependencies:
  - NodeState:Updating
  triggered by:
  - Node
  external states:
  - Node
`))
	})
})
