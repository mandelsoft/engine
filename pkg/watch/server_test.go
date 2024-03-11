package watch_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	. "github.com/mandelsoft/engine/pkg/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/server"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"

	_ "github.com/mandelsoft/engine/pkg/healthz"
)

var REALM = logging.NewRealm("engine/watch/client")
var log = logging.DefaultContext().Logger(REALM)

var _ = Describe("Watch Test Environment", func() {
	Context("", func() {
		var ctx context.Context
		var srv *server.Server
		var registry *Registry
		var services service.Services

		BeforeEach(func() {
			logcfg := logrusl.Human(true)
			logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

			lctx := logging.DefaultContext()
			lctx.AddRule(logging.NewConditionRule(logging.DebugLevel, logging.NewRealmPrefix("engine")))

			ctx = ctxutil.TimeoutContext(context.Background(), 30*time.Second)
			services = service.New(ctx)
			registry = NewRegistry()
			srv = server.NewServer(8080, true, 10*time.Second)
			services.Add(srv)

			services.Start()
		})

		AfterEach(func() {
			ctxutil.Cancel(ctx)
			MustBeSuccessful(services.Wait())
		})

		It("runs server", func() {
			srv.Handle("/watch", watch.WatchHttpHandler[RegistrationRequest, Event](registry))

			time.Sleep(time.Second)
			go func() {
				for i := 1; i < 10; i++ {
					registry.Trigger(Event{
						Key:     "test",
						Message: fmt.Sprintf("message %d", i),
					})
					time.Sleep(time.Second)
				}
				ctxutil.Cancel(ctx)
			}()

			s := Must(Consume())
			MustBeSuccessful(s.Wait())

			Expect("").To(Equal(""))
		})
	})
})

type RegistrationRequest struct {
	Key string `json:"key"`
}

type Event struct {
	Key     string `json:"key"`
	Message string `json:"message"`
}

type Handler = watch.EventHandler[Event]

type Registry struct {
	lock     sync.Mutex
	handlers map[string][]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: map[string][]Handler{},
	}
}

func (r *Registry) RegisterWatchHandler(req RegistrationRequest, h Handler) {
	r.lock.Lock()
	defer r.lock.Unlock()

	log.Info("registering handler for {{key}}", "key", req.Key)
	list := r.handlers[req.Key]
	r.handlers[req.Key] = append(list, h)
}

func (r *Registry) UnregisterWatchHandler(req RegistrationRequest, h Handler) {
	r.lock.Lock()
	defer r.lock.Unlock()

	log.Info("unregistering handler for {{key}}", "key", req.Key)
	r.handlers[req.Key] = utils.FilterSlice(r.handlers[req.Key], utils.NotFilter(utils.EqualsFilter(h)))
}

func (r *Registry) Trigger(evt Event) {
	r.lock.Lock()
	list := slices.Clone(r.handlers[evt.Key])
	r.lock.Unlock()

	log.Info("trigger event {{event}} for {{amount}} handlers", "event", evt, "amount", len(list))
	for _, h := range list {
		h.HandleEvent(evt)
	}
}

////////////////////////////////////////////////////////////////////////////////

func Consume() (watch.Syncher, error) {
	c := watch.NewClient[RegistrationRequest, Event]("ws://localhost:8080/watch")

	registration := RegistrationRequest{Key: "test"}
	return c.Register(context.Background(), registration, &handler{})
}

type handler struct {
}

func (h *handler) HandleEvent(e Event) {
	log.Info("got event {{event}}", "event", e)
}
