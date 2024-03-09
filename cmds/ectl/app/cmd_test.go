package app_test

import (
	"bytes"
	"context"
	"time"

	. "github.com/mandelsoft/engine/pkg/impl/database/filesystem/testtypes"
	. "github.com/mandelsoft/engine/pkg/testutils"
	"github.com/mandelsoft/vfs/pkg/vfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		access.Register(srv)
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
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns1       o1         
ns1       o2         
`))
		})
		It("get type elem", func() {
			cmd.SetArgs([]string{"get", "A", "ns1/o1"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(Equal(`
NAMESPACE NAME STATUS
ns1       o1         
`))
		})

		It("nothing", func() {
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "ns1/o1"})
			ExpectError(cmd.Execute()).To(MatchError("ns1/o1: request failed with status 404 Not Found"))
		})

		It("yaml", func() {
			cmd.SetArgs([]string{"-n", "ns1", "get", "A", "-o", "yaml"})
			MustBeSuccessful(cmd.Execute())
			Expect("\n" + buf.String()).To(YAMLEqual(`
  items:
  - a: A-ns1-o1
    generation: 0
    name: o1
    namespace: ns1
    type: A
  - a: A-ns1-o2
    generation: 0
    name: o2
    namespace: ns1
    type: A
`))
		})
	})

	Context("apply", func() {
		It("apply manifest", func() {
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
	})

})
