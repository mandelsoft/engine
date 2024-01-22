package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/ctxutil"
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

const CMD_EXT = "ext"

type NamespaceInfo struct {
	namespace common.Namespace
	elements  map[ElementId]Element
	internal  map[common.ObjectId]common.InternalObject
}

func NewNamespaceInfo(ns common.Namespace) *NamespaceInfo {
	return &NamespaceInfo{
		namespace: ns,
		elements:  map[ElementId]Element{},
		internal:  map[common.ObjectId]common.InternalObject{},
	}
}

func (ni *NamespaceInfo) GetNamespaceName() string {
	return ni.namespace.GetNamespaceName()
}

type Processor struct {
	lock sync.Mutex

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
			ns, err := p.assureNamespace(ons, true)
			if err != nil {
				return err
			}
			ns.internal[common.NewObjectIdFor(o)] = o
			curlock := ns.namespace.GetLock()

			if curlock != "" {
				// reset lock for all partially locked objects belonging to the locked run id.
				for _, ph := range p.mm.Phases(o.GetType()) {
					_, err := o.ClearLock(p.ob, ph, curlock)
					if err != nil {
						return err
					}
				}
			}

			for _, ph := range p.mm.Phases(o.GetType()) {
				e := NewElement(ph, o)
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

func (p *Processor) assureNamespace(name string, create bool) (*NamespaceInfo, error) {
	ns := p.namespaces[name]
	if ns == nil {
		nns, nn := NamespaceId(name)
		b, err := p.ob.GetObject(database.NewObjectId(p.mm.NamespaceType(), nns, nn))
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) || !create {
				return nil, err
			}
			b, err = p.ob.SchemeTypes().CreateObject(p.mm.NamespaceType(), objectbase.SetObjectName(nns, nn))
			if err != nil {
				return nil, err
			}
		}
		ns = NewNamespaceInfo(b.(common.Namespace))
		p.namespaces[name] = ns
	}
	return ns, nil
}

func (p *Processor) AssureElementObjectFor(e model.ExternalObject) (Element, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	t := p.mm.GetPhaseFor(e.GetType())
	if t == nil {
		return nil, fmt.Errorf("external object type %q not configured", e.GetType())
	}

	id := common.NewElementId(t.Type(), e.GetNamespace(), e.GetName(), t.Phase())

	ns, err := p.assureNamespace(id.Namespace(), true)
	if err != nil {
		return nil, err
	}

	elem := ns.elements[id]
	if elem != nil {
		return elem, nil
	}

	_i, err := p.ob.SchemeTypes().CreateObject(t.Type(), objectbase.SetObjectName(id.Namespace(), id.Name()))
	if err != nil {
		return nil, err
	}

	i := _i.(model.InternalObject)
	elem = NewElement(t.Phase(), i)

	ns.elements[id] = elem
	ns.internal[common.NewObjectIdFor(i)] = i
	return elem, nil
}

func (p *Processor) EnqueueKey(cmd string, id ElementId) {
	k := EncodeElement("ext", id)
	p.pool.EnqueueCommand(k)
}

func (p *Processor) Enqueue(cmd string, e Element) {
	k := EncodeElement("ext", e.Id())
	p.pool.EnqueueCommand(k)
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

	elem, err := p.AssureElementObjectFor(o)
	if err != nil {
		return pool.StatusFailed(err)
	}

	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}

func (p *Processor) processElement(lctx logging.Context, cmd string, id ElementId) pool.Status {
	return pool.StatusCompleted(nil)
}
