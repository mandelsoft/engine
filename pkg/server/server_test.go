package server_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/mandelsoft/engine/pkg/server"
	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/mandelsoft/engine/pkg/healthz"
)

var _ = Describe("Test Environment", func() {
	Context("", func() {
		var srv *server.Server

		BeforeEach(func() {
			srv = server.NewServer(8080, true)
			go func() {
				MustBeSuccessful(srv.ListenAndServe())
			}()
		})

		AfterEach(func() {
			srv.Shutdown(context.Background())
		})

		It("runs server", func() {
			srv.Handle("/test", http.HandlerFunc(testHandler))
			time.Sleep(100 * time.Second)
			Expect("").To(Equal(""))
		})
	})
})

func testHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "test handler\n")
}
