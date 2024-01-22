package processing

import (
	"fmt"
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processElement(lctx logging.Context, cmd string, id ElementId) pool.Status {
	lctx = lctx.WithContext(logging.NewAttribute("namespace", id.Namespace()), logging.NewAttribute("element", id))

	elem := p.GetElement(id)
	if elem == nil {
		return pool.StatusFailed(fmt.Errorf("unknown element %q", id))
	}

	runid := elem.GetLock()
	if runid == "" {
		if cmd == CMD_EXT {
			return p.checkChange(lctx, elem)
		}
	} else {
		return p.checkState(lctx.WithContext(logging.NewAttribute("runid", runid)), elem)
	}
	return pool.StatusCompleted()
}

func (p *Processor) checkChange(lctx logging.Context, e Element) pool.Status {
	log := lctx.Logger()
	types := p.mm.GetTriggerFor(e.Id().TypeId())
	if len(types) > 0 {
		log.Info("checking state of external objects for element {{element}}")
		for _, t := range types {
			id := database.NewObjectId(t, e.GetNamespace(), e.GetName())
			log := log.WithValues("extid", id)
			_o, err := p.ob.GetObject(id)
			if err != nil {
				log.Error("cannot get external object {{extid}}")
				continue
			}

			cur := e.GetCurrentState().GetVersion()
			o := _o.(model.ExternalObject)

			v := o.GetState().GetVersion()
			if v == cur {
				log.Info("state of external object {{extid}} not changed")
				continue
			}
			log.Info("state of {{extid}} changed from {{current}} to {{target}}", "current", cur, "target", v)
			log.Info("trying to trigger new run for {{element}}")

			rid, err := p.lockGraph(lctx, e)
			if err == nil {
				if rid != nil {
					log.Info("starting run {{runid}}", "runid", *rid)
					return pool.StatusRedo()
				} else {
					err = fmt.Errorf("delay initiation of new run")
				}
			}
			return pool.StatusCompleted(err)
		}
	}
	return pool.StatusCompleted()
}

func (p *Processor) checkState(lctx logging.Context, e Element) pool.Status {
	return pool.StatusCompleted()
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) lockGraph(lctx logging.Context, elem Element) (*model.RunId, error) {
	id := model.NewRunId()
	ns := p.GetNamespace(elem.GetNamespace())

	if !ns.lock.TryLock() {
		return nil, nil
	}
	defer ns.lock.Unlock()

	log := lctx.Logger(logging.NewAttribute("runid", id))
	ok, err := ns.namespace.TryLock(p.ob, id)
	if err != nil {
		log.Info("locking  namespace {{namespace}} for new runid {{runid}} failed", "error", err)
		return nil, err
	}
	if !ok {
		log.Info("cannot lock namespace {{namespace}} for already locked for {{current}}", "current", ns.namespace.GetLock())
		return nil, nil
	}
	log.Info("namespace {{namespace}} locked for new runid {{runid}}")
	defer func() {
		err := ns.clearLocks(log, p)
		if err != nil {
			log.Error("cannot clear namespace lock for {{namespace}} -> requeue", "error", err)
			p.EnqueueNamespace(ns.GetNamespaceName())
		}
	}()

	elems := map[ElementId]Element{}
	ok, err = p._tryLockGraph(log, ns, elem, elems)
	if !ok || err != nil {
		return nil, err
	}
	ok, err = p._lockGraph(log, ns, elems, id)
	if !ok || err != nil {
		return nil, err
	}
	return &id, nil
}

func (p *Processor) getChildren(ns *NamespaceInfo, elem Element) []Element {
	var r []Element
	id := elem.Id()
	for _, e := range ns.elements {
		state := e.GetCurrentState()
		if state != nil {
			if slices.Contains(state.GetLinks(), id) {
				r = append(r, e)
			}

		}
	}
	return r
}

func (p *Processor) _tryLockGraph(log logging.Logger, ns *NamespaceInfo, elem Element, elems map[ElementId]Element) (bool, error) {
	if elems[elem.Id()] == nil {
		cur := elem.GetLock()
		if cur != "" {
			return false, nil
		}
		elems[elem.Id()] = elem

		for _, d := range p.getChildren(ns, elem) {
			ok, err := p._tryLockGraph(log, ns, d, elems)
			if !ok || err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (p *Processor) _lockGraph(log logging.Logger, ns *NamespaceInfo, elems map[ElementId]Element, id model.RunId) (bool, error) {
	var ok bool
	var err error

	ns.pendingLocks = map[ElementId]Element{}

	for _, elem := range elems {
		ok, err = elem.TryLock(p.ob, id)
		if !ok || err != nil {
			return false, err
		}
		ns.pendingLocks[elem.Id()] = elem
	}
	ns.pendingLocks = nil
	return true, nil
}
