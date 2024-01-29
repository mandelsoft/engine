package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/healthz"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
	"k8s.io/client-go/util/workqueue"
)

var REALM = logging.DefineRealm("engine/pool", "processing worker pool for engine")

var poolkey = ""

type Pool interface {
	GetName() string
	Period() time.Duration

	AddAction(key ActionTargetSpec, a Action)
	GetActions(key interface{}) []Action

	Start(*sync.WaitGroup)
	Run()

	EnqueueCommand(cmd Command)
	EnqueueCommandRateLimited(cmd Command)
	EnqueueCommandAfter(cmd Command, duration time.Duration)

	EnqueueKey(key database.ObjectId)
	EnqueueKeyRateLimited(key database.ObjectId)
	EnqueueKeyAfter(key database.ObjectId, duration time.Duration)
}

type MessageContext logging.MessageContext

type pool struct {
	logging.UnboundLogger
	name       string
	size       int
	ctx        context.Context
	lctx       logging.Context
	period     time.Duration
	workqueue  Queue
	actions    *actionMapping
	useKeyName bool
	key        string
}

func NewPool(ctx context.Context, lctx logging.Context, name string, size int, period time.Duration, useKeyName ...bool) Pool {
	lctx = lctx.WithContext(REALM, logging.NewAttribute("pool", name))
	pool := &pool{
		UnboundLogger: logging.DynamicLogger(lctx),
		name:          name,
		size:          size,
		period:        period,
		lctx:          lctx,
		useKeyName:    utils.Optional(useKeyName...),
		key:           fmt.Sprintf("pool %s", name),
		workqueue: workqueue.NewRateLimitingQueueWithConfig(workqueue.DefaultControllerRateLimiter(), workqueue.RateLimitingQueueConfig{
			Name: name,
		}),
		actions: newActionMapping(),
	}

	pool.UnboundLogger = logging.DynamicLogger(lctx, logging.NewAttribute("pool", name))
	pool.ctx = ctxutil.WaitGroupContext(
		context.WithValue(ctx, &poolkey, pool),
		fmt.Sprintf("pool %s", name),
	)

	if pool.period != 0 {
		pool.Info("created pool", "name", pool.name, "size", pool.size, "resync period", pool.period.String())
	} else {
		pool.Info("created pool", "name", pool.name, "size", pool.size)
	}
	return pool
}

func (p *pool) AddAction(key ActionTargetSpec, a Action) {
	p.Info("adding action", "type", fmt.Sprintf("%T", a), "key", key.String())
	p.actions.addAction(key, a)
}

func (p *pool) GetActions(key interface{}) []Action {
	return p.actions.getAction(key)
}

func (p *pool) GetName() string {
	return p.name
}

func (p *pool) GetWorkqueue() Queue {
	return p.workqueue
}

func (p *pool) Key() string {
	return p.key
}

func (p *pool) Period() time.Duration {
	return p.period
}

func (p *pool) QueueLength() int {
	return p.workqueue.Len()
}

func (p *pool) Tick() {
	healthz.Tick(p.Key())
}

func (p *pool) Start(wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		p.Run()
	}()
}

func (p *pool) Run() {
	p.Info("starting worker pool", "name", p.name, "workers", p.size)
	period := p.period
	if period == 0 {
		p.Info("no reconcile period active -> start ticker")
		period = tick
	}
	// always run periodic tickCmd to deal with empty workqueue
	p.workqueue.AddAfter(tickCmd, period)

	healthz.Start(p.Key(), period)
	for i := 0; i < p.size; i++ {
		p.startWorker(i, p.ctx.Done())
	}

	<-p.ctx.Done()
	p.workqueue.ShutDown()
	p.Info("waiting for pool workers to shutdown", "name", p.name)
	ctxutil.WaitGroupWait(p.ctx, 120*time.Second)
	healthz.End(p.Key())
}

func (p *pool) startWorker(number int, stopCh <-chan struct{}) {
	ctxutil.WaitGroupRunUntilCancelled(p.ctx, func() { newWorker(p, number).Run() })
}
func (p *pool) EnqueueCommand(cmd Command) {
	p.enqueueCommand(cmd, p.workqueue.Add)
}
func (p *pool) EnqueueCommandRateLimited(name Command) {
	p.enqueueCommand(name, p.workqueue.AddRateLimited)
}
func (p *pool) EnqueueCommandAfter(name Command, duration time.Duration) {
	p.enqueueCommand(name, func(key interface{}) { p.workqueue.AddAfter(key, duration) })
}
func (p *pool) enqueueCommand(cmd Command, add func(interface{})) {
	add(EncodeCommandKey(cmd))
}

func (p *pool) EnqueueKey(key database.ObjectId) {
	p.enqueueKey(key, p.workqueue.Add)
}
func (p *pool) EnqueueKeyRateLimited(key database.ObjectId) {
	p.enqueueKey(key, p.workqueue.AddRateLimited)
}
func (p *pool) EnqueueKeyAfter(key database.ObjectId, duration time.Duration) {
	p.enqueueKey(key, func(key interface{}) { p.workqueue.AddAfter(key, duration) })
}
func (p *pool) enqueueKey(key database.ObjectId, add func(interface{})) {
	okey := EncodeObjectKeyForObject(key)
	add(okey)
}
