package processing

import (
	"context"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

type Processor struct {
	ctx     context.Context
	logging logging.Context
	mm      metamodel.MetaModel
	db      objectbase.Objectbase
	pool    pool.Pool
	handler database.EventHandler

	elements map[ElementId]Element
}

func NewProcessor(ctx context.Context, lctx logging.Context, m model.Model, worker int) (*Processor, error) {
	pool := pool.NewPool(ctxutil.CancelContext(ctx), lctx, m.MetaModel().Name(), worker, 0)
	return &Processor{
		ctx:     ctx,
		logging: lctx,
		mm:      m.MetaModel(),
		db:      m.Objectbase(),
		pool:    pool,

		elements: map[ElementId]Element{},
	}, nil
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

	p.db.RegisterHandler(reg, true, "")
	return nil
}

func (p *Processor) setupElements() error {

	namespaces := map[string]common.Namespace{}
	for _, t := range p.mm.InternalTypes() {
		objs, err := p.db.ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, o := range objs {
			ns := namespaces[o.GetNamespace()]
			if ns == nil {
				b, err := p.db.GetObject(database.NewObjectId(p.mm.NamespaceType(), "", o.GetNamespace()))
				if err != nil {
					return err
				}
				ns = b.(common.Namespace)
			}
			for _, ph := range p.mm.Phases(o.GetType()) {
				i := o.(model.InternalObject)
				e := NewElement(ph, i)
				p.elements[e.id] = e

			}
			_ = o
		}
	}
	return nil
}
