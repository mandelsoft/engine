package common_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

var _ = Describe("version", func() {
	It("strings ids", func() {
		id := me.NewObjectId("a", "b", "c")
		Expect(fmt.Sprintf("%s", id)).To(Equal("a/b/c"))
	})

})
