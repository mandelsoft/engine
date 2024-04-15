package processor

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/server"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("engine/processor", "engine processor")

const FINALIZER = "engine"

const CMD_EXT = "ext"
const CMD_ELEM = "elem"
const CMD_NS = "ns"

type Controller struct {
	lctx   logging.Context
	worker int

	processingModel *processingModel
	composer        version.Composer

	ctx     context.Context
	logging logging.Context
	pool    pool.Pool
	syncher service.Syncher
	ready   service.Trigger
	handler database.EventHandler

	events  *EventManager
	pending PendingCounter

	delay time.Duration
}

var _ service.Service = (*Controller)(nil)

func NewController(lctx logging.Context, m model.Model, worker int, cmps ...version.Composer) (*Controller, error) {
	p := &Controller{
		pool:            pool.NewPool(lctx, m.MetaModel().Name(), worker, 0, false),
		logging:         lctx.WithContext(REALM),
		processingModel: newProcessingModel(m),
		composer:        general.OptionalDefaulted[version.Composer](version.Composed, cmps...),
	}
	p.events = newEventManager(p.processingModel)
	return p, nil
}

func (p *Controller) RegisterWatchHandler(s *server.Server, pattern string) {
	s.Handle(pattern, watch.WatchHttpHandler[elemwatch.Request, elemwatch.Event](p.events.registry))
}

func (p *Controller) RegisterHandler(handler EventHandler, current bool, kind string, closure bool, ns string) {
	p.events.RegisterHandler(handler, current, kind, closure, ns)
}

func (p *Controller) UnregisterHandler(handler EventHandler, kind string, closure bool, ns string) {
	p.events.UnregisterHandler(handler, kind, closure, ns)
}

func (p *Controller) Model() ProcessingModel {
	return p.processingModel
}

func (p *Controller) WaitFor(ctx context.Context, etype EventType, id ElementId) bool {
	return p.events.Wait(ctx, etype, id)
}

func (p *Controller) FutureFor(etype EventType, id ElementId, retrigger ...bool) Future {
	return p.events.Future(etype, id, retrigger...)
}

func (p *Controller) getNamespaceInfo(name string) *namespaceInfo {
	n, _ := p.processingModel.AssureNamespace(p.logging.Logger(), name, false)
	return n
}

func (p *Controller) Wait() error {
	return p.syncher.Wait()
}

func (p *Controller) SetDelay(d time.Duration) {
	p.delay = d
}

func (p *Controller) MetaModel() metamodel.MetaModel {
	return p.processingModel.MetaModel()
}

func (p *Controller) Objectbase() objectbase.Objectbase {
	return p.processingModel.ObjectBase()
}

func (p *Controller) Start(ctx context.Context) (service.Syncher, service.Syncher, error) {
	if p.syncher != nil {
		return p.ready, p.syncher, nil
	}
	log := p.logging.Logger().WithName("setup")

	err := p.setupElements(p.logging.AttributionContext(), log)
	if err != nil {
		return nil, nil, err
	}

	p.handler = newHandler(p.pool)

	extReconcile := newExternalObjectReconciler(p)
	reg := database.NewHandlerRegistry(p.processingModel.ObjectBase())
	reg.RegisterHandler(p.handler, false, p.processingModel.MetaModel().NamespaceType(), true, "/")
	for _, t := range p.processingModel.MetaModel().ExternalTypes() {
		log.Debug("register handler for external type {{exttype}}", "exttype", t)
		reg.RegisterHandler(p.handler, false, t, true, "/")
		p.pool.AddAction(pool.ObjectType(t), extReconcile)
	}

	if req := p.processingModel.MetaModel().UpdateRequestType(); req != "" {
		log.Debug("register handler for update request type {{reqtype}}", "reqtype", req)
		reg.RegisterHandler(p.handler, true, req, true, "/")
		p.pool.AddAction(pool.ObjectType(req), newUpdateRequestReconciler(p))
	}

	elemReconcile := newElementReconciler(p)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_NS+":*"), newNamespaceReconciler(p))
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_ELEM+":*"), elemReconcile)
	p.pool.AddAction(utils.NewStringGlobMatcher(CMD_EXT+":*"), elemReconcile)

	p.processingModel.ObjectBase().RegisterHandler(reg, true, "", true, "")

	ready, sy, err := p.pool.Start(ctx)
	if err != nil {
		return nil, nil, err
	}
	p.syncher = sy

	err = ready.Wait()
	if err != nil {
		return nil, nil, err
	}

	p.ready = service.SyncTrigger()

	go func() {
		log.Info("triggering all elements")
		c := 0
		for _, n := range p.processingModel.Namespaces() {
			for _, id := range p.processingModel.GetNamespace(n).Elements() {
				p.EnqueueKey(CMD_ELEM, id)
				c++
			}
		}
		log.Info("{{amount}} elements triggered", "amount", c)
		p.ready.Trigger()
	}()
	return p.ready, sy, nil
}

type setup struct {
	reconciler
	logging.Logger
	lctx      model.Logging
	namespace string
}

var _ Reconcilation = (*setup)(nil)

func (s *setup) LoggingContext() model.Logging {
	return s.lctx
}

func (s *setup) GetNamespace() string {
	return s.namespace
}

func (p *Controller) setupElements(lctx model.Logging, log logging.Logger) error {
	// step 1: create processing elements and cleanup pending locks
	log.Info("setup internal objects...")

	for _, t := range p.MetaModel().InternalTypes() {
		log.Debug("  for type {{inttype}}", "inttype", t)
		objs, err := p.Objectbase().ListObjects(t, true, "")
		if err != nil {
			return err
		}

		for _, _o := range objs {
			log.Debug("    found {{intid}}", "intid", database.NewObjectIdFor(_o))
			o := _o.(model.InternalObject)
			r := &setup{
				reconciler: reconciler{controller: p},
				Logger:     log,
				lctx:       lctx,
				namespace:  o.GetNamespace(),
			}
			ni, err := p.processingModel.AssureNamespace(r, r.GetNamespace(), true)
			if err != nil {
				return err
			}
			ni.internal[mmids.NewObjectIdFor(o)] = o
			curlock := ni.namespace.GetLock()

			for _, ph := range p.MetaModel().Phases(o.GetType()) {
				log.Debug("      found phase {{phase}}", "phase", ph)
				e := ni._AddElement(o, ph)
				if curlock != "" {
					if owner := IsObjectLock(curlock); owner != nil {
						id := (*owner).Id(o.GetNamespace(), p.processingModel.MetaModel())
						log.Info("triggering locked request {{oid}}", "oid", id)
						if inProcess(e) {
							e.SetProcessingState(NewTargetState(e))
						}
						p.EnqueueObject(id)
					} else {
						// reset lock for all partially locked objects belonging to the locked run id.
						err := ni.clearElementLock(r, e, curlock)
						if err != nil {
							return err
						}
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
					log.Warn("element {{element}} has unknown linked element {{link}}", "element", e.Id(), "link", l)
				}
			}
			// target state must not already be linked
		}
	}

	return nil
}

func (p *Controller) setupNewInternalObject(log logging.Logger, ni *namespaceInfo, i model.InternalObject, phase Phase, runid RunId) Element {
	var elem Element
	log.Info("setup new internal object {{id}} for required phase {{reqphase}}", "id", NewObjectIdFor(i), "reqphase", phase)
	tolock, _ := p.processingModel.MetaModel().GetDependentTypePhases(NewTypeId(i.GetType(), phase))
	for _, ph := range p.processingModel.MetaModel().Phases(i.GetType()) {
		n := ni._AddElement(i, ph)
		log.Info("  setup new phase {{newelem}}", "newelem", n.Id())
		if ph == phase {
			elem = n
		}
		if slices.Contains(tolock, ph) {
			ok, err := n.TryLock(p.processingModel.ObjectBase(), runid)
			if !ok { // new object should already be locked correctly provide atomic phase creation
				panic(fmt.Sprintf("cannot lock incorrectly locked new element: %s", err))
			}
			log.Info("  dependent phase {{depphase}} locked", "depphase", ph)
		}
	}
	return elem
}

func (p *Controller) EnqueueKey(cmd string, id ElementId) {
	k := EncodeElement(cmd, id)
	p.pool.EnqueueCommand(k)
}

func (p *Controller) Enqueue(cmd string, e Element) {
	k := EncodeElement(cmd, e.Id())
	p.pool.EnqueueCommand(k)
}

func (p *Controller) EnqueueNamespace(name string) {
	p.pool.EnqueueCommand(EncodeNamespace(name))
}

func (p *Controller) EnqueueObject(id database.ObjectId) {
	p.pool.EnqueueKey(id)
}

func (p *Controller) GetElement(id ElementId) _Element {
	return p.processingModel._GetElement(id)
}

func (p *Controller) setStatus(log logging.Logger, e _Element, status model.Status, trigger ...bool) error {
	ok, err := e.SetStatus(p.processingModel.ObjectBase(), status)
	if err != nil {
		return err
	}
	if ok {
		log.Info("status updated to {{status}} for {{element}}", "status", status, "element", e.Id())
	}
	if ok || general.Optional(trigger...) {
		p.events.TriggerStatusEvent(log, e)
	}
	return nil
}

type Reconciler interface {
	Controller() *Controller
	TriggerElementEvent(elem _Element)
	TriggerNamespaceEvent(ni *namespaceInfo)
}

type controller = *Controller

type reconciler struct {
	pool.DefaultAction
	controller
}

func (r *reconciler) Controller() *Controller {
	return r.controller
}

func (r *reconciler) TriggerElementEvent(elem _Element) {
	r.events.TriggerElementEvent(elem)
}

func (r *reconciler) TriggerNamespaceEvent(ni *namespaceInfo) {
	r.events.TriggerNamespaceEvent(ni)
}

type Reconcilation interface {
	logging.Logger
	Reconciler

	MetaModel() metamodel.MetaModel
	Objectbase() objectbase.Objectbase
	Controller() *Controller

	LoggingContext() model.Logging
	GetNamespace() string
}
