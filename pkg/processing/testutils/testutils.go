package testutils

import (
	"bytes"
	"context"
	"sync"

	. "github.com/mandelsoft/engine/pkg/testutils"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

type TestEnv struct {
	wg     *sync.WaitGroup
	fs     vfs.FileSystem
	ctx    context.Context
	logbuf *bytes.Buffer
	db     database.Database[support.DBObject]
	proc   *processor.Processor
}

type Waitable interface {
	Wait(ctx context.Context) bool
}

type ModelCreator func(name string, dbspec database.Specification[support.DBObject]) model.ModelSpecification

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

	spec := creator(name, filesystem.NewSpecification[support.DBObject](path, fs))
	err = spec.Validate()
	if err != nil {
		vfs.Cleanup(fs)
		return nil, err
	}

	logbuf := bytes.NewBuffer(nil)
	logcfg := logrusl.Human(true)
	logging.DefaultContext().SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

	lctx := logging.DefaultContext()
	lctx.AddRule(logging.NewConditionRule(options.debugLevel, logging.NewRealmPrefix("engine/processor")))
	lctx.AddRule(logging.NewConditionRule(options.debugLevel, logging.NewRealmPrefix("database")))

	ctx := ctxutil.CancelContext(context.Background())

	m, err := model.NewModel(spec)
	if err != nil {
		vfs.Cleanup(fs)
		return nil, err
	}
	proc := Must(processor.NewProcessor(ctx, lctx, m, options.numWorker))
	db := objectbase.GetDatabase[support.DBObject](proc.Model().ObjectBase())

	return &TestEnv{
		fs:     fs,
		ctx:    ctx,
		logbuf: logbuf,
		db:     db,
		proc:   proc,
	}, nil
}

func (t *TestEnv) Context() context.Context {
	return t.ctx
}

func (t *TestEnv) Processor() *processor.Processor {
	return t.proc
}

func (t *TestEnv) Database() database.Database[support.DBObject] {
	return t.db
}

func (t *TestEnv) Start() {
	if t.wg == nil {
		t.wg = &sync.WaitGroup{}
		t.proc.Start(t.wg)
	}
}

func (t *TestEnv) GetObject(id database.ObjectId) (support.DBObject, error) {
	return t.db.GetObject(id)
}

func (t *TestEnv) SetObject(o support.DBObject) error {
	return t.db.SetObject(o)
}

func (t *TestEnv) CompletedFuture(id mmids.ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(model.STATUS_COMPLETED, id, retrigger...)
}

func (t *TestEnv) DeletedFuture(id mmids.ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(model.STATUS_DELETED, id, retrigger...)
}

func (t *TestEnv) FutureFor(etype processor.EventType, id mmids.ElementId, retrigger ...bool) processor.Future {
	return t.proc.FutureFor(etype, id, retrigger...)
}

func (t *TestEnv) Wait(w Waitable) bool {
	return w.Wait(t.ctx)
}

func (t *TestEnv) Cleanup() {
	ctxutil.Cancel(t.ctx)
	if t.wg != nil {
		t.wg.Wait()
	}
	vfs.Cleanup(t.fs)
}
