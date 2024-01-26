package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("engine/processor", "engine processor")

const CMD_EXT = "ext"
const CMD_ELEM = "elem"
const CMD_NS = "ns"

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

	events  *EventManager
	pending PendingCounter
}

func NewProcessor(ctx context.Context, lctx logging.Context, m model.Model, worker int) (*Processor, error) {
	pool := pool.NewPool(ctx, lctx, m.MetaModel().Name(), worker, 0)
	return &Processor{
		ctx:     ctx,
		logging: lctx.WithContext(REALM),
		m:       m,
		mm:      m.MetaModel(),
		ob:      m.Objectbase(),
		pool:    pool,

		events:     NewEventManager(),
		namespaces: map[string]*NamespaceInfo{},
	}, nil
}

func (p *Processor) Wait(ctx context.Context) bool {
	return p.pending.Wait(ctx)
}

func (p *Processor) WaitForCompleted(ctx context.Context, id ElementId) bool {
	return p.events.Wait(ctx, id)
}

func (p *Processor) GetNamespace(name string) *NamespaceInfo {
	p.lock.Lock()
	defer p.lock.Unlock()

	n, _ := p.assureNamespace(p.logging.Logger(), name, false)
	return n

}
func (p *Processor) AssureNamespace(log logging.Logger, name string, create bool) (*NamespaceInfo, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.assureNamespace(log, name, create)
}

func (p *Processor) assureNamespace(log logging.Logger, name string, create bool) (*NamespaceInfo, error) {
	ns := p.namespaces[name]
	if ns == nil {
		nns, nn := NamespaceId(name)
		b, err := p.ob.GetObject(database.NewObjectId(p.mm.NamespaceType(), nns, nn))
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) || !create {
				log.Error("cannot get namespace object for {{namespace}}", "namespace", name)
				return nil, err
			}
			log.Info("creating namespace object for {{namespace}}", "namespace", name)
			b, err = p.ob.SchemeTypes().CreateObject(p.mm.NamespaceType(), objectbase.SetObjectName(nns, nn))
			if err != nil {
				log.Error("cannot create namespace object for {{namespace}}", "namespace", name)
				return nil, err
			}
		} else {
			log.Info("found namespace object for {{namespace}}", "namespace", name)
		}
		ns = NewNamespaceInfo(b.(common.Namespace))
		p.namespaces[name] = ns
	}
	return ns, nil
}

func (p *Processor) MetaModel() metamodel.MetaModel {
	return p.mm
}

func (p *Processor) Objectbase() objectbase.Objectbase {
	return p.ob
}

func (p *Processor) Start(wg *sync.WaitGroup) error {
	if p.handler != nil {
		return nil
	}

	log := p.logging.Logger().WithName("setup")

	err := p.setupElements(log)
	if err != nil {
		return err
	}

	p.handler = newHandler(p.pool)

	act := &action{p}
	reg := database.NewHandlerRegistry(nil)
	reg.RegisterHandler(p.handler, false, p.mm.NamespaceType())
	for _, t := range p.mm.ExternalTypes() {
		log.Debug("register handler for external type {{exttype}}", "exttype", t)
		reg.RegisterHandler(p.handler, false, t)
		p.pool.AddAction(pool.ObjectType(t), act)
	}
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_NS+":*"), act)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_ELEM+":*"), act)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_EXT+":*"), act)

	p.ob.RegisterHandler(reg, true, "")

	p.pool.Start(wg)
	return nil
}

func (p *Processor) setupElements(log logging.Logger) error {
	// step 1: create processing elements and cleanup pending locks
	log.Info("setup internal objects...")
	for _, t := range p.mm.InternalTypes() {
		log.Debug("  for type {{inttype}}", "inttype", t)
		objs, err := p.ob.ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, _o := range objs {
			log.Debug("    found {{intid}}", "intid", database.NewObjectIdFor(_o))
			o := _o.(model.InternalObject)
			ons := o.GetNamespace()
			ni, err := p.assureNamespace(log, ons, true)
			if err != nil {
				return err
			}
			ni.internal[common.NewObjectIdFor(o)] = o
			curlock := ni.namespace.GetLock()

			for _, ph := range p.mm.Phases(o.GetType()) {
				log.Debug("      found phase {{phase}}", "phase", ph)
				e := ni.AddElement(o, ph)
				if curlock != "" {
					// reset lock for all partially locked objects belonging to the locked run id.
					err := ni.clearElementLock(log, p, e, curlock)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	// step 2: validate links
	log.Info("validating linḱs...")
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

func (p *Processor) AssureElementObjectFor(log logging.Logger, e model.ExternalObject) (Element, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	t := p.mm.GetPhaseFor(e.GetType())
	if t == nil {
		return nil, fmt.Errorf("external object type %q not configured", e.GetType())
	}

	eid := common.NewObjectIdFor(e)
	log = log.WithValues("extid", eid)
	id := common.NewElementId(t.Type(), e.GetNamespace(), e.GetName(), t.Phase())

	ns, err := p.assureNamespace(log, id.Namespace(), true)
	if err != nil {
		return nil, err
	}

	elem := ns.elements[id]
	if elem != nil {
		return elem, nil
	}

	log.Info("creating internal object for {{extid}}")
	_i, err := p.ob.SchemeTypes().CreateObject(t.Type(), objectbase.SetObjectName(id.Namespace(), id.Name()))
	if err != nil {
		log.Error("creation of internal object for external object {{extid}} failed", "error", err)
		return nil, err
	}

	i := _i.(model.InternalObject)
	elem = NewElement(t.Phase(), i)

	ns.elements[id] = elem
	ns.internal[common.NewObjectIdFor(i)] = i
	return elem, nil
}

func (p *Processor) EnqueueKey(cmd string, id ElementId) {
	k := EncodeElement(cmd, id)
	p.pool.EnqueueCommand(k)
}

func (p *Processor) Enqueue(cmd string, e Element) {
	k := EncodeElement(cmd, e.Id())
	p.pool.EnqueueCommand(k)
}

func (p *Processor) EnqueueNamespace(name string) {
	p.pool.EnqueueCommand(EncodeNamespace(name))
}

func (p *Processor) GetElement(id ElementId) Element {
	p.lock.Lock()
	defer p.lock.Unlock()

	ns := p.namespaces[id.Namespace()]
	if ns == nil {
		return nil
	}
	return ns.elements[id]
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) processExternalObject(log logging.Logger, id database.ObjectId) pool.Status {

	_o, err := p.ob.GetObject(id)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			// TODO: object deleted
		}
		return pool.StatusFailed(err)
	}
	o := _o.(model.ExternalObject)

	elem, err := p.AssureElementObjectFor(log, o)
	if err != nil {
		return pool.StatusFailed(err)
	}

	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}
