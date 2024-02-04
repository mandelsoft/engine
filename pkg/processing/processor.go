package processing

import (
	"context"
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("engine/processor", "engine processor")

const CMD_EXT = "ext"
const CMD_ELEM = "elem"
const CMD_NS = "ns"

type Processor struct {
	processingModel *processingModel

	ctx     context.Context
	logging logging.Context
	pool    pool.Pool
	handler database.EventHandler

	events  *EventManager
	pending PendingCounter
}

func NewProcessor(ctx context.Context, lctx logging.Context, m model.Model, worker int) (*Processor, error) {
	pool := pool.NewPool(ctx, lctx, m.MetaModel().Name(), worker, 0, false)
	return &Processor{
		ctx:             ctx,
		logging:         lctx.WithContext(REALM),
		processingModel: newProcessingModel(m),
		pool:            pool,

		events: NewEventManager(),
	}, nil
}

func (p *Processor) Model() ProcessingModel {
	return p.processingModel
}

func (p *Processor) Wait(ctx context.Context) bool {
	return p.pending.Wait(ctx)
}

func (p *Processor) WaitForCompleted(ctx context.Context, id ElementId) bool {
	return p.events.Wait(ctx, id)
}

func (p *Processor) CompletedFuture(id ElementId, retrigger ...bool) Future {
	return p.events.Future(id, retrigger...)
}

func (p *Processor) getNamespace(name string) *namespaceInfo {
	n, _ := p.processingModel.AssureNamespace(p.logging.Logger(), name, false)
	return n
}

func (p *Processor) Start(wg *sync.WaitGroup) error {
	if p.handler != nil {
		return nil
	}

	log := p.logging.Logger().WithName("setup")

	err := p.setupElements(p.logging.AttributionContext(), log)
	if err != nil {
		return err
	}

	p.handler = newHandler(p.pool)

	act := &action{p}
	reg := database.NewHandlerRegistry(nil)
	reg.RegisterHandler(p.handler, false, p.processingModel.MetaModel().NamespaceType())
	for _, t := range p.processingModel.MetaModel().ExternalTypes() {
		log.Debug("register handler for external type {{exttype}}", "exttype", t)
		reg.RegisterHandler(p.handler, false, t)
		p.pool.AddAction(pool.ObjectType(t), act)
	}
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_NS+":*"), act)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_ELEM+":*"), act)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_EXT+":*"), act)

	p.processingModel.ObjectBase().RegisterHandler(reg, true, "")

	p.pool.Start(wg)
	return nil
}

func (p *Processor) setupElements(lctx common.Logging, log logging.Logger) error {
	// step 1: create processing elements and cleanup pending locks
	log.Info("setup internal objects...")
	for _, t := range p.processingModel.MetaModel().InternalTypes() {
		log.Debug("  for type {{inttype}}", "inttype", t)
		objs, err := p.processingModel.ObjectBase().ListObjects(t, "")
		if err != nil {
			return err
		}

		for _, _o := range objs {
			log.Debug("    found {{intid}}", "intid", database.NewObjectIdFor(_o))
			o := _o.(model.InternalObject)
			ons := o.GetNamespace()
			ni, err := p.processingModel.AssureNamespace(log, ons, true)
			if err != nil {
				return err
			}
			ni.internal[common.NewObjectIdFor(o)] = o
			curlock := ni.namespace.GetLock()

			for _, ph := range p.processingModel.MetaModel().Phases(o.GetType()) {
				log.Debug("      found phase {{phase}}", "phase", ph)
				e := ni.AddElement(o, ph)
				if curlock != "" {
					// reset lock for all partially locked objects belonging to the locked run id.
					err := ni.clearElementLock(lctx, log, p, e, curlock)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	// step 2: validate links
	log.Info("validating linḱs...")
	for _, ns := range p.processingModel.namespaces {
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
