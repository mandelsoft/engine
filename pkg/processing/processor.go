package processing

import (
	"context"
	"fmt"

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
	ob      objectbase.Objectbase
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
		ob:      m.Objectbase(),
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

	p.ob.RegisterHandler(reg, true, "")
	return nil
}

func (p *Processor) setupElements() error {

	namespaces := map[string]common.Namespace{}

	// step 1: create processing elements and cleanup pending locks
	for _, t := range p.mm.InternalTypes() {
		objs, err := p.ob.ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, _o := range objs {
			o := _o.(model.InternalObject)
			ons := o.GetNamespace()
			ns := namespaces[ons]
			curlock := ns.GetLock()
			if ns == nil {
				b, err := p.ob.GetObject(database.NewObjectId(p.mm.NamespaceType(), ParentNamespace(ons), NamespaceName(ons)))
				if err != nil {
					return err
				}
				ns = b.(common.Namespace)
			}

			if curlock != "" {
				// reset lock for all partially locked objects belonging to the locked run id.
				_, err := objectbase.Modify(p.ob, o, func(o common.InternalObject) (bool, bool) {
					mod := false
					for _, ph := range p.mm.Phases(o.GetType()) {
						if o.GetLock(ph) == curlock {
							o.ClearLock(ph)
							mod = true
						}
					}
					return mod, mod
				})
				if err != nil {
					return err
				}
			}

			for _, ph := range p.mm.Phases(o.GetType()) {
				e := NewElement(ph, o)
				p.elements[e.id] = e
			}
		}
	}

	// step 2: links processing elements
	for _, t := range p.mm.InternalTypes() {
		objs, err := p.ob.ListObjects(t, "")
		if err != nil {
			return err
		}
		for _, _o := range objs {
			o := _o.(model.InternalObject)

			for _, ph := range p.mm.Phases(o.GetType()) {
				s := p.elements[common.NewElementIdForPhase(o, ph)]
				state := o.GetState()
				if state != nil {
					for _, l := range state.GetLinks(ph) {
						e := p.elements[l]
						if e == nil {
							return fmt.Errorf("state: object %q phase %q links to non-existing element %q", database.NewObjectIdFor(o), ph, l)
						}
						s.GetCurrentState().AddLink(l)
					}
				}
				if o.GetLock(ph) != "" {
					state := o.GetTargetState()
					if state != nil {
						for _, l := range state.GetLinks(ph) {
							e := p.elements[l]
							if e == nil {
								return fmt.Errorf("target state: object %q phase %q links to non-existing element %q", database.NewObjectIdFor(o), ph, l)
							}
							s.GetTargetState().AddLink(l)
						}
					}
				}
			}
		}
	}
	return nil
}
