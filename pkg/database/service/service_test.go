package service_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	. "github.com/mandelsoft/engine/pkg/impl/database/filesystem/testtypes"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/service"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/server"
	service2 "github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

var PORT = 8080
var URL = fmt.Sprintf("http://localhost:%d/db/", PORT)

const NS = "ns1"

var _ = Describe("Test Environment", func() {
	var ctx context.Context

	var srv *server.Server
	var done service2.Syncher

	var db database.Database[Object]
	var fs vfs.FileSystem
	var access *service.DatabaseAccess[Object]

	BeforeEach(func() {
		ctx = ctxutil.TimeoutContext(context.Background(), 20*time.Second)
		fs = Must(TestFileSystem("testdata", false))
		db = Must(filesystem.New[Object](Scheme.(database.Encoding[Object]), "testdata", fs)) // Goland
		srv = server.NewServer(PORT, true, 10*time.Second)
		access = service.New(db, "/db")
		access.RegisterHandler(srv)
		ready, d := Must2(srv.Start(ctx))
		ready.Wait()
		done = d
	})

	AfterEach(func() {
		MustBeSuccessful(srv.Shutdown(ctx))
		done.Wait()
	})

	Context("get", func() {
		It("", func() {
			get := Must(http.Get(URL + path.Join(TYPE_A, NS, "o1")))
			Expect(get.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(get.Body)).To(YAMLEqual(`
type: A
namespace: ns1
name: o1
generation: 0

a: A-ns1-o1
`))
		})
	})

	Context("create", func() {
		It("", func() {
			data := `
type: A
namespace: ns1
name: new
a: new object
`

			post := Must(http.Post(URL+path.Join(TYPE_A, NS, "new"), "application/json", bytes.NewReader([]byte(data))))
			Expect(post.StatusCode).To(Equal(http.StatusCreated))

			get := Must(http.Get(URL + path.Join(TYPE_A, NS, "new")))
			Expect(get.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(get.Body)).To(YAMLEqual(`
type: A
namespace: ns1
name: new
generation: 1

a: new object
`))
		})

		It("list ns", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join(TYPE_A, NS), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
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

		It("list all types in ns", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join("*", NS), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
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
  - b: B-ns1-o1
    generation: 0
    name: o1
    namespace: ns1
    type: B
`))
		})

		It("list all", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join("*", "*"), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
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
  - a: A-ns2-o1
    generation: 0
    name: o1
    namespace: ns2
    type: A
  - b: B-ns1-o1
    generation: 0
    name: o1
    namespace: ns1
    type: B
  - b: B-ns1/sub1-o1
    generation: 0
    name: o1
    namespace: ns1/sub1
    type: B
  - b: B-ns2-o2
    generation: 0
    name: o2
    namespace: ns2
    type: B
`))
		})

	})
})
