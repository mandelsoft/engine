package delivery_test

import (
	"bytes"

	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/processing/objectbase"

	me "github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

var _ = Describe("meta model", func() {
	It("validate", func() {

		spec := me.NewModelSpecification("test", nil)
		Expect(spec.Validate()).To(Succeed())
	})

	It("initializes", func() {

		spec := me.NewModelSpecification("test", nil)
		types := spec.Objectbase.SchemeTypes()

		o := Must(types.CreateObject(mymetamodel.TYPE_OPERATOR, objectbase.SetObjectName("namespace", "test")))

		Expect(o.GetName()).To(Equal("test"))
		Expect(o.GetNamespace()).To(Equal("namespace"))
		Expect(o.GetType()).To(Equal(mymetamodel.TYPE_OPERATOR))

	})

	It("evaluates", func() {
		spec := me.NewModelSpecification("test", nil)

		buf := &bytes.Buffer{}
		m := Must(spec.GetMetaModel())
		m.Dump(buf)

		Expect("\n" + buf.String()).To(Equal(`
Namespace type: Namespace
External types:
- Operator  (-> OperatorState:Gathering)
  internal type: OperatorState
  phase:         Gathering
- Value  (-> ValueState:Propagating)
  internal type: ValueState
  phase:         Propagating
Internal types:
- OperatorState
  phases:
  - Calculating
  - Gathering
  trigger types:
  - Operator
- ValueState
  phases:
  - Propagating
  trigger types:
  - Value
Element types:
- OperatorState:Calculating
  dependencies:
  - OperatorState:Gathering (local)
  updated states:
  - Operator
- OperatorState:Gathering
  triggered by: Operator
  dependencies:
  - ValueState:Propagating
  updated states:
  - Operator
- ValueState:Propagating
  triggered by: Value
  dependencies:
  - OperatorState:Calculating
  updated states:
  - Value
`))
	})
})
