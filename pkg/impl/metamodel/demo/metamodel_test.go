package demo_test

import (
	"github.com/mandelsoft/engine/pkg/impl/metamodel/demo"
	"github.com/mandelsoft/engine/pkg/metamodel"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("meta model", func() {
	It("validate", func() {

		mm := demo.NewMetaMode()
		Expect(metamodel.Validate(mm)).To(Succeed())
	})
})
