package foreigndemo_test

import (
	"bytes"
	"fmt"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"

	me "github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
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

		fmt.Printf("\n%s\n", buf.String())
		Expect("\n" + buf.String()).To(Equal(`
Namespace type: Namespace
External types:
- Expression  (-> OperatorState:Calculating)
  internal type: OperatorState
  phase:         Calculating
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
  - Expression
  - Operator
- ValueState
  phases:
  - Propagating
  trigger types:
  - Value
Element types:
- OperatorState:Calculating
  dependencies:
  - OperatorState:Gathering
  triggered by:
  - Expression
  external states:
  - Expression
  - Operator
- OperatorState:Gathering
  dependencies:
  - ValueState:Propagating
  triggered by:
  - Operator
  external states:
  - Operator
- ValueState:Propagating
  dependencies:
  - OperatorState:Calculating
  triggered by:
  - Value
  external states:
  - Value
`))
	})
})
