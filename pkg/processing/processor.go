package processing

import (
	"context"
	"errors"
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

type NamespaceInfo struct {
	namespace common.Namespace
	elements  map[ElementId]Element
	internal  map[common.ObjectId]*common.InternalObject
}

func NewNamespaceInfo(ns common.Namespace) *NamespaceInfo {
	return &NamespaceInfo{
		namespace: ns,
		elements:  map[ElementId]Element{},
		internal:  map[common.ObjectId]*common.InternalObject{},
	}
}

func (ni *NamespaceInfo) GetNamespaceName() string {
	return ni.namespace.GetNamespaceName()
}

type Processor struct {
	ctx     context.Context
	logging logging.Context
	m       model.Model
	mm      metamodel.MetaModel
	ob      objectbase.Objectbase
	pool    pool.Pool
	handler database.EventHandler

	namespaces map[string]*NamespaceInfo
}

func NewProcessor(ctx context.Context, lctx logging.Context, m model.Model, worker int) (*Processor, error) {
	pool := pool.NewPool(ctxutil.CancelContext(ctx), lctx, m.MetaModel().Name(), worker, 0)
	return &Processor{
		ctx:     ctx,
		logging: lctx,
		m:       m,
		mm:      m.MetaModel(),
		ob:      m.Objectbase(),
		pool:    pool,

		namespaces: map[string]*NamespaceInfo{},
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

	p.handler = newHandler(p.pool)

	reg := database.NewHandlerRegistry(nil)
	reg.RegisterHandler(p.handler, false, p.mm.NamespaceType())
	for _, t := range p.mm.ExternalTypes() {
		reg.RegisterHandler(p.handler, false, t)
	}

	p.ob.RegisterHandler(reg, true, "")
	return nil
}

func (p *Processor) setupElements() error {
	// step 1: create processing elements and cleanup pending locks
	for _, t := range p.mm.InternalTypes() {
		objs, err := p.ob.ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, _o := range objs {
			o := _o.(model.InternalObject)
			ons := o.GetNamespace()
			ns := p.namespaces[ons]
			if ns == nil {
				b, err := p.ob.GetObject(database.NewObjectId(p.mm.NamespaceType(), ParentNamespace(ons), NamespaceName(ons)))
				if err != nil {
					return err
				}
				ns = NewNamespaceInfo(b.(common.Namespace))
				p.namespaces[o.GetNamespace()] = ns
			}
			ns.internal[common.NewObjectIdFor(o)] = &o
			curlock := ns.namespace.GetLock()

			if curlock != "" {
				// reset lock for all partially locked objects belonging to the locked run id.
				_, err := objectbase.Modify(p.ob, &o, func(o common.InternalObject) (bool, bool) {
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
				e := NewElement(ph, &o)
				ns.elements[e.id] = e
			}
		}
	}

	// step 2: validate links

	for _, ns := range p.namespaces {
		for _, e := range ns.elements {
			for _, l := range e.GetCurrentState().GetLinks() {
				if ns.elements[l] == nil {
					return fmt.Errorf("%s: unknown linked element %q", e.Id(), l)
				}
			}
			// target state must not already be linked
		}
	}
	return nil
}

func (p *Processor) GetInternalObjectFor(e model.ExternalObject) (*metamodel.TypeId, *model.InternalObject, Element) {
	t := p.mm.GetPhaseFor(e.GetType())
	if t == nil {
		return nil, nil, nil
	}

	id := common.NewElementId(t.Type(), e.GetNamespace(), e.GetName(), t.Phase())

	ns := p.namespaces[id.Namespace()]
	if ns == nil {
		return t, nil, nil
	}
	elem := ns.elements[id]
	if elem != nil {
		return t, elem.GetObject(), elem
	}
	return t, ns.internal[id.ObjectId()], nil
}

func (p *Processor) processExternalObject(lctx logging.Context, id database.ObjectId) pool.Status {

	_o, err := p.ob.GetObject(id)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			// TODO: object deleted
		}
		return pool.StatusFailed(err)
	}
	o := _o.(model.ExternalObject)

	if t, i, _ := p.GetInternalObjectFor(o); i != nil {
		// add new internal object
		_i, err := p.m.SchemeTypes().CreateObject(t.Type())
		if err != nil {
			return pool.StatusFailed(err)
		}
		_ = _i
	}
	if a, ok := o.(common.RunAwareObject); ok {
		_ = a
	}
	return pool.StatusCompleted(nil)
}

func (p *Processor) processElement(lctx logging.Context, cmd string, id ElementId) pool.Status {
	return pool.StatusCompleted(nil)
}
