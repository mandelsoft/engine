package watch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/mandelsoft/engine/pkg/server"
	. "github.com/mandelsoft/engine/pkg/testutils"
	"github.com/mandelsoft/engine/watch"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/mandelsoft/engine/pkg/healthz"
)

var REALM = logging.NewRealm("engine/watch/client")
var log = logging.DefaultContext().Logger(REALM)

var _ = Describe("Watch Test Environment", func() {
	Context("", func() {
		var srv *server.Server
		var registry *Registry

		BeforeEach(func() {
			logcfg := logrusl.Human(true)
			logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

			lctx := logging.DefaultContext()
			lctx.AddRule(logging.NewConditionRule(logging.DebugLevel, logging.NewRealmPrefix("engine")))

			srv = server.NewServer(8080, true)
			registry = NewRegistry()
			go func() {
				MustBeSuccessful(srv.ListenAndServe())
			}()
		})

		AfterEach(func() {
			srv.Shutdown(context.Background())
		})

		It("runs server", func() {
			srv.Handle("/watch", watch.WatchRequest[RegistrationRequest, Event](registry))

			time.Sleep(time.Second)
			go func() {
				for i := 1; i < 100; i++ {
					registry.Trigger(Event{
						Key:     "test",
						Message: fmt.Sprintf("message %d", i),
					})
					time.Sleep(time.Second)
				}
			}()

			go func() {
				err := Consume()
				fmt.Printf("consume: %s\n", err)
			}()

			time.Sleep(100 * time.Second)
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
func (r *Registry) Register(req RegistrationRequest, h Handler) {
	r.lock.Lock()
	defer r.lock.Unlock()

	log.Info("registering handler for {{key}}", "key", req.Key)
	list := r.handlers[req.Key]
	r.handlers[req.Key] = append(list, h)
}

func (r *Registry) Trigger(evt Event) {
	r.lock.Lock()
	list := slices.Clone(r.handlers[evt.Key])
	r.lock.Unlock()

	log.Info("trigger event {{event}} for {{amount}} handlers", "event", evt, "amount", len(list))
	for _, h := range list {
		h.Handle(evt)
	}
}

////////////////////////////////////////////////////////////////////////////////

func Consume() error {
	conn, _, _, err := ws.Dial(context.Background(), "ws://localhost:8080/watch")

	if err != nil {
		return err
	}

	registration := RegistrationRequest{Key: "test"}

	data, _ := json.Marshal(registration)
	err = wsutil.WriteClientMessage(conn, ws.OpBinary, data)
	if err != nil {
		return err
	}

	for {
		msgs, err := wsutil.ReadServerMessage(conn, nil)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			var evt Event

			err := json.Unmarshal(m.Payload, &evt)
			if err != nil {
				return err
			}
			fmt.Printf("%#v\n", evt)
		}
	}
}
