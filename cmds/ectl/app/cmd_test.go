package app_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	. "github.com/mandelsoft/engine/pkg/database/service/testtypes"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/mandelsoft/goutils/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/vfs/pkg/vfs"
	"github.com/spf13/cobra"

	"github.com/mandelsoft/engine/cmds/ectl/app"
	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/service"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/server"
	service2 "github.com/mandelsoft/engine/pkg/service"
)

const PORT = 8080

var _ = Describe("Test Environment", func() {
	var ctx context.Context

	var srv *server.Server
	var done service2.Syncher

	var db database.Database[Object]
	var fs vfs.FileSystem
	var access *service.DatabaseAccess[Object]

	var cmd *cobra.Command
	var buf *bytes.Buffer

	BeforeEach(func() {
		ctx = ctxutil.TimeoutContext(context.Background(), 20*time.Second)
		fs = Must(TestFileSystem("testdata", false))
		db = Must(filesystem.New[Object](Scheme.(database.Encoding[Object]), "testdata/db", fs)) // Goland
		srv = server.NewServer(PORT, true, 10*time.Second)
		access = service.New(db, "/db")
		access.RegisterHandler(srv)
		ready, d := Must2(srv.Start(ctx))
		ready.Wait()
		done = d

		buf = bytes.NewBuffer(nil)
		cmd = app.New(fs)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
	})

	AfterEach(func() {
		MustBeSuccessful(srv.Shutdown(ctx))
		done.Wait()
	})

	Context("get", func() {
		It("get type", func() {
			cmd.SetArgs([]string{"-n", "ns1", "get", "A"})
			MustBeSuccessful(cmd.Execute())
			fmt.Printf("\n%s\n", buf.String())
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns1       o1   Completed
ns1       o2
`))
		})
		It("get type elem", func() {
			cmd.SetArgs([]string{"get", "A", "ns1/o1"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns1       o1   Completed
`))
		})

		It("nothing", func() {
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "ns1/o1"})
			ExpectError(cmd.Execute()).To(MatchError("ns1/o1: object not found"))
		})

		It("yaml", func() {
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "-o", "yaml"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(YAMLEqual(`
 items:
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      a: A-ns1-o1
    status:
      status: Completed
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o2
      namespace: ns1
      finalizers:
      - test
    spec:
      a: A-ns1-o2
`))
		})
	})

	Context("apply", func() {
		It("create", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"apply", "-f", "testdata/new.yaml"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
B/ns2/new: created
`))
			buf.Reset()
			cmd.SetArgs([]string{"-n", "ns2", "get", "B", "new"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns2       new
`))
		})

		It("update", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"apply", "-f", "testdata/update.yaml"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
A/ns1/o1: updated
`))
			buf.Reset()
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "o1"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns1       o1   Completed
`))
			buf.Reset()
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "o1", "-o", "yaml"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(YAMLEqual(`
    apiVersion: engine/v1
    kind: A
    metadata:
      generation: 1
      name: o1
      namespace: ns1
    spec:
      a: modified
    status:
      status: Completed
`))
		})
	})

	Context("delete", func() {
		It("delete object", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"-n", "ns1", "delete", "A", "o1"})
			MustBeSuccessful(cmd.Execute())
			fmt.Printf("\n%s\n", buf.String())
			Expect("\n" + buf.String()).To(Equal(`
A/ns1/o1: deleted
`))
		})

		It("requests deletion", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"-n", "ns1", "delete", "A", "o2"})
			MustBeSuccessful(cmd.Execute())
			fmt.Printf("\n%s\n", buf.String())
			Expect("\n" + buf.String()).To(Equal(`
A/ns1/o2: deletion requested
`))
		})
		It("forces deletion", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"-n", "ns1", "delete", "--force", "A", "o2"})
			MustBeSuccessful(cmd.Execute())
			fmt.Printf("\n%s\n", buf.String())
			Expect("\n" + buf.String()).To(Equal(`
A/ns1/o2: deletion enforced
`))
		})
		It("handles files", func() {
			cmd.SetOut(buf)
			cmd.SetArgs([]string{"-n", "ns1", "delete", "-f", "testdata/update.yaml"})
			MustBeSuccessful(cmd.Execute())
			fmt.Printf("\n%s\n", buf.String())
			Expect("\n" + buf.String()).To(Equal(`
A/ns1/o1: deleted
`))
		})
	})
})
