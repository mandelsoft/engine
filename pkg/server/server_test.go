package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/server"
	"github.com/mandelsoft/engine/pkg/service"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/mandelsoft/engine/pkg/healthz"
)

var _ = Describe("Test Environment", func() {
	Context("", func() {
		var ctx context.Context
		var srv *server.Server
		var done service.Syncher
		var ready service.Syncher

		BeforeEach(func() {
			ctx = ctxutil.TimeoutContext(context.Background(), 10*time.Second)
			srv = server.NewServer(8080, true, 10*time.Second)
			ready, done = Must2(srv.Start(ctx))
		})

		AfterEach(func() {
			srv.Shutdown(ctx)
			done.Wait()
		})

		It("runs server", func() {
			srv.Handle("/test", http.HandlerFunc(testHandler))

			MustBeSuccessful(ready.Wait())
			resp := Must(http.Get("http://localhost:8080/test"))
			data := Must(io.ReadAll(resp.Body))
			Expect(string(data)).To(Equal("test handler\n"))
		})
	})
})

func testHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "test handler\n")
}
