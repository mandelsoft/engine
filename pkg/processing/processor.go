package processing

import (
	"context"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

type Processor struct {
	ctx     context.Context
	logging logging.Context
	mm      metamodel.MetaModel
	db      database.Database
	pool    pool.Pool
	handler database.EventHandler
}

func NewProcessor(ctx context.Context, lctx logging.Context, m model.Model, worker int) (*Processor, error) {
	pool := pool.NewPool(ctxutil.CancelContext(ctx), lctx, m.MetaModel().Name(), worker, 0)
	return &Processor{ctx: ctx, logging: lctx, mm: m.MetaModel(), db: m.Database(), pool: pool}, nil
}

func (p *Processor) Start() error {
	if p.handler == nil {
		return nil
	}

	err := p.setupElements()
	if err != nil {

		return err
	}

	p.handler = newHandler(p)

	reg := database.NewHandlerRegistry(nil)
	reg.RegisterHandler(p.handler, false, p.mm.NamespaceType())
	for _, t := range p.mm.ExternalTypes() {
		reg.RegisterHandler(p.handler, false, t)
	}
	for _, t := range p.mm.InternalTypes() {
		reg.RegisterHandler(p.handler, false, t)
	}

	p.db.RegisterHandler(reg, true, "")
	return nil
}

func (p *Processor) setupElements() error {

	for _, t := range p.mm.InternalTypes() {
		objs, err := p.db.ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, o := range objs {
			_ = o
		}
	}
	return nil
}
