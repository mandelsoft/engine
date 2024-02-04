package mmids

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("version", func() {
	It("strings ids", func() {
		id := NewObjectId("a", "b", "c")
		Expect(fmt.Sprintf("%s", id)).To(Equal("a/b/c"))
	})

})
