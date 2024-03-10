package service_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	. "github.com/mandelsoft/engine/pkg/database/service/testtypes"
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
apiVersion: engine/v1
kind: A
metadata:
  namespace: ns1
  name: o1
  generation: 0
spec:
  a: A-ns1-o1
`))
		})
	})

	Context("create", func() {
		It("", func() {
			data := `
apiVersion: engine/v1
kind: A
metadata:
  namespace: ns1
  name: new
spec:
  a: new object
`

			post := Must(http.Post(URL+path.Join(TYPE_A, NS, "new"), "application/json", bytes.NewReader([]byte(data))))
			Expect(post.StatusCode).To(Equal(http.StatusCreated))

			get := Must(http.Get(URL + path.Join(TYPE_A, NS, "new")))
			Expect(get.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(get.Body)).To(YAMLEqual(`
apiVersion: engine/v1
kind: A
metadata:
  namespace: ns1
  name: new
  generation: 1
spec:
  a: new object
`))
		})

		It("list ns", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join(TYPE_A, NS), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
  items:
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      a: A-ns1-o1
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o2
      namespace: ns1
    spec:
      a: A-ns1-o2
`))
		})

		It("list all types in ns", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join("*", NS), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
  items:
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      a: A-ns1-o1
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o2
      namespace: ns1
    spec:
      a: A-ns1-o2
  - apiVersion: engine/v1
    kind: B
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      b: B-ns1-o1
`))
		})

		It("list all", func() {
			req := Must(http.NewRequest("LIST", URL+path.Join("*", "*"), nil))
			list := Must(http.DefaultClient.Do(req))
			Expect(list.StatusCode).To(Equal(http.StatusOK))
			Expect(io.ReadAll(list.Body)).To(YAMLEqual(`
  items:
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      a: A-ns1-o1
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o2
      namespace: ns1
    spec:
      a: A-ns1-o2
  - apiVersion: engine/v1
    kind: A
    metadata:
      generation: 0
      name: o1
      namespace: ns2
    spec:
      a: A-ns2-o1
  - apiVersion: engine/v1
    kind: B
    metadata:
      generation: 0
      name: o1
      namespace: ns1
    spec:
      b: B-ns1-o1
  - apiVersion: engine/v1
    kind: B
    metadata:
      generation: 0
      name: o1
      namespace: ns1/sub1
    spec:
      b: B-ns1/sub1-o1
  - apiVersion: engine/v1
    kind: B
    metadata:
      generation: 0
      name: o2
      namespace: ns2
    spec:
      b: B-ns2-o2
`))
		})

	})
})
