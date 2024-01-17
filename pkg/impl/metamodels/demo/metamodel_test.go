package demo_test

import (
	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("meta model", func() {
	It("validate", func() {

		mm := demo.NewInstance("test")
		Expect(mm.Validate()).To(Succeed())
	})
})
