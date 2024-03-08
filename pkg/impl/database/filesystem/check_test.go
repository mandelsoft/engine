package filesystem_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
)

var _ = Describe("database", func() {

	Context("name check", func() {
		It("names", func() {
			Expect(filesystem.CheckName("A")).To(BeTrue())
			Expect(filesystem.CheckName("Abc")).To(BeTrue())
			Expect(filesystem.CheckName("A12")).To(BeTrue())
			Expect(filesystem.CheckName("A-_12-")).To(BeTrue())

			Expect(filesystem.CheckName("-A-_12-")).To(BeFalse())
		})

		It("namespace", func() {
			Expect(filesystem.CheckNamespace("A")).To(BeTrue())
			Expect(filesystem.CheckNamespace("Abc")).To(BeTrue())
			Expect(filesystem.CheckNamespace("A12")).To(BeTrue())
			Expect(filesystem.CheckNamespace("A-_12-")).To(BeTrue())

			Expect(filesystem.CheckNamespace("-A-_12-")).To(BeFalse())

			Expect(filesystem.CheckNamespace("a/A")).To(BeTrue())
			Expect(filesystem.CheckNamespace("a/Abc")).To(BeTrue())
			Expect(filesystem.CheckNamespace("a/A12")).To(BeTrue())
			Expect(filesystem.CheckNamespace("a/A-_12-")).To(BeTrue())

			Expect(filesystem.CheckNamespace("a/-A-_12-")).To(BeFalse())

			Expect(filesystem.CheckNamespace("a/A/b")).To(BeTrue())

		})
	})
})
