package testutils

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/service"
	. "github.com/mandelsoft/engine/pkg/testutils"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
	"github.com/mandelsoft/vfs/pkg/vfs"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/future"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/processor"
)

var log = logging.DefaultContext().Logger(logging.NewRealm("testenv"))

type Startable = service.Service

type TestEnv struct {
	lock         sync.Mutex
	services     service.Services
	fs           vfs.FileSystem
	ctx          context.Context
	lctx         logging.Context
	logbuf       *bytes.Buffer
	db           database.Database[db.Object]
	proc         *processor.Processor
	startables   []Startable
	started      bool
	objectStatus future.EventManager[ObjectId, model.Status]
}

type Waitable interface {
	Wait(ctx context.Context) bool
}

type ModelCreator func(name string, dbspec database.Specification[db.Object]) model.ModelSpecification

func NewTestEnv(name string, path string, creator ModelCreator, opts ...Option) (*TestEnv, error) {
	options := &Options{
		numWorker:  1,
		debugLevel: logging.TraceLevel,
	}

	for _, o := range opts {
		o.ApplyTo(options)
	}

	fs, err := TestFileSystem(path, false)
	if err != nil {
		return nil, err
	}

	spec := creator(name, filesystem.NewSpecification[db.Object](path, fs))
	err = spec.Validate()
	if err != nil {
		vfs.Cleanup(fs)
		return nil, err
	}

	logbuf := bytes.NewBuffer(nil)
	logcfg := logrusl.Human(true)
	logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

	lctx := logging.DefaultContext()
	lctx.AddRule(logging.NewConditionRule(options.debugLevel, logging.NewRealmPrefix("engine")))
	lctx.AddRule(logging.NewConditionRule(options.debugLevel, logging.NewRealmPrefix("database")))

	ctx := ctxutil.CancelContext(context.Background())

	m, err := model.NewModel(spec)
	if err != nil {
		vfs.Cleanup(fs)
		return nil, err
	}
	proc := Must(processor.NewProcessor(lctx, m, options.numWorker))
	db := objectbase.GetDatabase[db.Object](proc.Model().ObjectBase())

	mgr := future.NewEventManager[ObjectId, model.Status]()

	srvs := service.New(ctx)
	srvs.Add(proc)
	db.RegisterHandler(&handler{db, mgr}, false, "")
	return &TestEnv{
		services:     srvs,
		fs:           fs,
		ctx:          ctx,
		lctx:         lctx,
		logbuf:       logbuf,
		db:           db,
		proc:         proc,
		objectStatus: mgr,
	}, nil
}

func (t *TestEnv) Context() context.Context {
	return t.ctx
}

func (t *TestEnv) Logging() logging.Context {
	return t.lctx
}

func (t *TestEnv) Processor() *processor.Processor {
	return t.proc
}

func (t *TestEnv) Database() database.Database[db.Object] {
	return t.db
}

func (t *TestEnv) MetaModel() metamodel.MetaModel {
	return t.proc.Model().MetaModel()
}

func (t *TestEnv) AddService(s Startable) error {
	return t.services.Add(s)
}

func (t *TestEnv) Start(st ...Startable) error {
	return t.services.Start(st...)
}

func (t *TestEnv) List(typ string, ns string) ([]database.ObjectId, error) {
	return t.db.ListObjectIds(typ, ns)
}

func (t *TestEnv) GetObject(id database.ObjectId) (db.Object, error) {
	return t.db.GetObject(id)
}

func (t *TestEnv) SetObject(o db.Object) error {
	return t.db.SetObject(o)
}

func (t *TestEnv) DeleteObject(id database.ObjectId) error {
	return t.db.DeleteObject(id)
}

func (t *TestEnv) CompletedFuture(id ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(model.STATUS_COMPLETED, id, retrigger...)
}

func (t *TestEnv) DeletedFuture(id ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(model.STATUS_DELETED, id, retrigger...)
}

func (t *TestEnv) FutureFor(etype processor.EventType, id ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(etype, id, retrigger...)
}

func (t *TestEnv) Wait(w Waitable) bool {
	return w.Wait(t.ctx)
}

func (t *TestEnv) WaitWithTimeout(w Waitable) bool {
	ctx := ctxutil.TimeoutContext(t.ctx, 20*time.Second)
	return w.Wait(ctx)
}

func Modify[O db.Object, R any](env *TestEnv, o *O, mod func(o O) (R, bool)) (R, error) {
	return database.Modify(env.db, o, mod)
}

func (t *TestEnv) Cleanup() {
	ctxutil.Cancel(t.ctx)
	t.services.Wait()
	vfs.Cleanup(t.fs)
}

type handler struct {
	db  database.Database[db.Object]
	mgr future.EventManager[ObjectId, model.Status]
}

var _ database.EventHandler = (*handler)(nil)

const STATUS_ANY = model.Status("any")

func (h *handler) HandleEvent(id database.ObjectId) {
	o, err := h.db.GetObject(id)
	if errors.Is(err, database.ErrNotExist) {
		h.mgr.Trigger(log, model.STATUS_DELETED, NewObjectIdFor(id))
	} else {
		if err == nil {
			if s, ok := o.(database.StatusSource); ok {
				h.mgr.Trigger(log, model.Status(s.GetStatusValue()), NewObjectIdFor(id))
			}
			h.mgr.Trigger(log, STATUS_ANY, NewObjectIdFor(id))
		}
	}
}

func (t *TestEnv) FutureForObjectStatus(s model.Status, id database.ObjectId, retrigger ...bool) future.Future {
	return t.objectStatus.Future(s, NewObjectIdFor(id), retrigger...)
}

func (t *TestEnv) WaitForObjectStatus(s model.Status, id database.ObjectId) bool {
	return t.objectStatus.Wait(t.ctx, s, NewObjectIdFor(id))
}
