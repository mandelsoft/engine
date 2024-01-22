package pool_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	me "github.com/mandelsoft/engine/pkg/pool"
)

var CMD_TEST = me.Command("test")
var CMD_SCHED = me.Command("schedule")

type action struct {
	lock    sync.Mutex
	actions []me.Command
	ids     []database.ObjectId
}

var _ me.Action = (*action)(nil)

func (a *action) Reconcile(p me.Pool, l logging.Context, id database.ObjectId) me.Status {
	a.lock.Lock()
	defer a.lock.Unlock()

	first := !slices.Contains(a.ids, id)
	a.ids = append(a.ids, id)
	_ = first
	return me.StatusCompleted()
}

func (a *action) Command(p me.Pool, log logging.Context, command me.Command) me.Status {
	a.lock.Lock()
	defer a.lock.Unlock()

	first := !slices.Contains(a.actions, command)
	a.actions = append(a.actions, command)

	i := strings.Index(command.String(), ":")
	if i > 0 {
		key := command[:i]
		sub := string(command)[i+1:]

		switch key {
		case CMD_TEST:
			n, err := strconv.Atoi(sub)
			if err == nil {
				if n > 0 {
					p.EnqueueCommand(me.Command(fmt.Sprintf("%s:%d", key, n-1)))
				}
			}
		case CMD_SCHED:
			n, err := strconv.Atoi(sub)
			if err == nil {
				if first {
					if n > 0 {
						return me.StatusCompleted().RescheduleAfter(time.Second * time.Duration(n))
					} else {
						return me.StatusRedo()
					}
				}
			} else {
				return me.StatusFailed(fmt.Errorf("%s", sub))
			}
		}
	}
	return me.StatusCompleted()
}

var _ = Describe("version", func() {
	var pool me.Pool
	var wg *sync.WaitGroup
	var ctx context.Context

	BeforeEach(func() {
		ctx = ctxutil.CancelContext(context.Background())
		pool = me.NewPool(ctx, logging.DefaultContext(), "test", 1, 0)
		wg = &sync.WaitGroup{}
	})

	AfterEach(func() {
		ctxutil.Cancel(ctx)
		wg.Wait()
	})

	Context("commad", func() {
		It("command", func() {
			a := &action{}
			pool.AddAction(CMD_TEST, a)

			pool.Start(wg)
			pool.EnqueueCommand(CMD_TEST)

			time.Sleep(time.Second)
			Expect(a.actions).To(ConsistOf(CMD_TEST))
			Expect(a.ids).To(BeNil())
		})

		It("schedule other command", func() {
			a := &action{}
			pool.AddAction(utils.NewStringGlobMatcher(string(CMD_TEST)+":*"), a)

			pool.Start(wg)
			pool.EnqueueCommand(CMD_TEST + ":2")

			time.Sleep(time.Second)
			Expect(a.actions).To(ConsistOf(CMD_TEST+":2", CMD_TEST+":1", CMD_TEST+":0"))
			Expect(a.ids).To(BeNil())
		})

		It("repeat command", func() {
			a := &action{}
			pool.AddAction(utils.NewStringGlobMatcher(string(CMD_SCHED)+":*"), a)

			pool.Start(wg)
			pool.EnqueueCommand(CMD_SCHED + ":0")

			time.Sleep(time.Second)
			Expect(a.actions).To(ConsistOf(CMD_SCHED+":0", CMD_SCHED+":0"))
			Expect(a.ids).To(BeNil())
		})

		It("reschedule command", func() {
			a := &action{}
			pool.AddAction(utils.NewStringGlobMatcher(string(CMD_SCHED)+":*"), a)

			pool.Start(wg)
			pool.EnqueueCommand(CMD_SCHED + ":1")

			time.Sleep(2 * time.Second)
			Expect(a.actions).To(ConsistOf(CMD_SCHED+":1", CMD_SCHED+":1"))
			Expect(a.ids).To(BeNil())
		})
	})

	Context("object", func() {
		It("handles id", func() {
			a := &action{}
			pool.AddAction(me.ObjectType("type"), a)
			pool.Start(wg)

			id := database.NewObjectId("type", "ns", "object")
			pool.EnqueueKey(id)

			time.Sleep(2 * time.Second)
			Expect(a.ids).To(ConsistOf(id))
			Expect(a.actions).To(BeNil())
		})
	})
})
