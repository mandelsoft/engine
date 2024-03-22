package processor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/stringutils"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processUpdateRequest(lctx model.Logging, log logging.Logger, id database.ObjectId) pool.Status {
	oid := NewObjectIdFor(id)
	m := p.processingModel
	log = log.WithName(oid.String()).WithValues("reqid", oid)
	ob := p.processingModel.ob

	var ni *namespaceInfo
	tmp := m.GetNamespace(id.GetNamespace())
	if tmp == nil {
		ni = nil
	} else {
		ni = tmp.(*namespaceInfo)
		ni.lock.Lock()
		defer ni.lock.Unlock()
	}

	o, err := m.ob.GetObject(oid)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return pool.StatusCompleted(err)
		}
		log.Info("request deleted")
		// object deleted -> unlock
		if ni == nil {
			return pool.StatusCompleted()
		}
		return pool.StatusCompleted(p.clearNamespaceLockForObject(log, ni, id))
	}
	ur := o.(model.UpdateRequestObject)

	status := ur.GetStatus()
	action := ur.GetAction()
	if action.Action == model.REQ_ACTION_RELEASE &&
		(status.Status == model.REQ_STATUS_RELEASED || status.Status == model.REQ_STATUS_INVALID) {
		log.Info("request already done")
		return pool.StatusCompleted(p.clearNamespaceLockForObject(log, ni, id))
	}

	if err := validateUpdateRequest(m.mm, action); err != nil {
		log.Error("invalid request: {{message}}", "message", err.Error())
		r := p.clearNamespaceLockForObject(log, ni, id)
		if r != nil {
			return pool.StatusCompleted(err)
		}
		mod, r := setRequestStatus(ur, ob, model.REQ_STATUS_INVALID, err.Error())
		if mod {
			log.Info("updated status to {{status}}: {{message}}", "status", model.REQ_STATUS_INVALID, "message", err.Error())
		}
		return pool.StatusCompleted(r)
	}

	ni, err = p.processingModel.assureNamespace(log, o.GetNamespace(), true)
	if err != nil {
		return pool.StatusCompleted(err)
	}

	// step 1: assure namespace locked
	runid := runIdForObject(ni, id)
	log = log.WithValues("runid", runid)
	log.Info("step 1: trying to acquire namespace lock for request {{reqid}}: {{runid}}")

	ok, err := ni.tryLock(p, runid)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	if !ok {
		return pool.StatusCompleted(fmt.Errorf("retry aquiring namespace lock"))
	}
	log.Info("  acquired namespace lock {{runid}}")
	switch status.Status {
	case model.REQ_STATUS_RELEASED, "":
		_, err := setRequestStatus(ur, ob, model.REQ_STATUS_ACQUIRED, "namespace locked")
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}

	// step 2: lock elements
	switch action.Action {
	case model.REQ_ACTION_LOCK, model.REQ_ACTION_RELEASE:
		var elems []_Element

		for _, es := range action.Objects {
			tid := p.processingModel.MetaModel().GetPhaseFor(es.Type)
			eid := NewElementIdForType(*tid, id.GetNamespace(), es.Name)
			e := ni._getElement(eid)
			if e != nil {
				elems = append(elems, e)
			}
		}
		log.Info("step 2: locking elements for {{runid}}; {{elements}}", "elements", stringutils.Join(action.Objects, ", "))
		ok, err := p.doLockGraph(log, ni, runid, true, elems...)
		if err != nil {
			if ok {
				log.Info("  elements not yet completely lockable")
				_, err = setRequestStatus(ur, ob, model.REQ_STATUS_PENDING, "waiting for lockable elements")
				if err != nil {
					return pool.StatusCompleted(err)
				}
			}
			return pool.StatusCompleted(err)
		}
		_, err = setRequestStatus(ur, ob, model.REQ_STATUS_LOCKED, "elements locked")
		if err != nil {
			return pool.StatusCompleted(err)
		}
		log.Info("  elements locked {{elements}}", "elements", stringutils.Join(action.Objects, ", "))
	}

	// step 3: release locks
	switch action.Action {
	case model.REQ_ACTION_RELEASE:
		log.Info("step 3: releasing namespace lock {{runid}}")
		for _, es := range action.Objects {
			tid := p.processingModel.MetaModel().GetPhaseFor(es.Type)
			eid := NewElementIdForType(*tid, id.GetNamespace(), es.Name)
			e := ni._getElement(eid)
			if e == nil {
				oid := es.In(o.GetNamespace())
				_, err := ob.GetObject(oid)
				if err != nil {
					if !errors.Is(err, database.ErrNotExist) {
						return pool.StatusCompleted(err)
					}
					log.Info("- object {{oid}} does not exist", "oid", oid)
					_, err = setRequestStatus(ur, ob, model.REQ_STATUS_INVALID, fmt.Sprintf("object %q does not exist", oid))
					if err != nil {
						return pool.StatusCompleted(err)
					}
					return pool.StatusCompleted(p.clearNamespaceLockForObject(log, ni, ur))
				}
				log.Info("- object {{oid}} exists, but is not yet reconciled -> dely", "oid", oid)
				return pool.StatusCompleted(fmt.Errorf("waiting for object %q to be reconciled", oid))
			}
			log.Info("- object {{oid}} ready for release", "oid", oid)
		}

		log.Info("all objects ready: releasing namespace lock {{runid}}")
		err := p.clearNamespaceLockForObject(log, ni, id)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		_, err = setRequestStatus(ur, ob, model.REQ_STATUS_RELEASED, "elements locked and namespace released")
		if err != nil {
			return pool.StatusCompleted(err)
		}
		log.Info("  released namespace lock {{runid}}")
	}

	return pool.StatusCompleted()
}

func setRequestStatus(ur model.UpdateRequestObject, ob objectbase.Objectbase, status string, message string) (bool, error) {
	snew := *ur.GetStatus()
	snew.Status = status
	snew.Message = message
	snew.ObservedVersion = ur.GetAction().Version()
	return ur.SetStatus(ob, &snew)
}

func (p *Processor) clearNamespaceLockForObject(log logging.Logger, ni *namespaceInfo, id database.ObjectId) error {
	if ni == nil {
		return nil
	}
	runid := ni.namespace.GetLock()
	owner := IsObjectLock(runid)
	if owner != nil && database.CompareObject((*owner).Id(id.GetNamespace(), p.processingModel.mm), id) == 0 {
		log.Info("clear namespace lock {{runid}}", "runid", runid)
		err := ni.clearLock(log, runid, p)
		if err != nil {
			return err
		}
		log.Info("trigger elements for runid {{runid}}", "runid", runid)
		for eid := range ni.filterElements(func(e _Element) bool { return e.GetLock() == runid }) {
			log.Info("- trigger element {{eid}}", "eid", eid)
			p.EnqueueKey(CMD_ELEM, eid)
		}
	}
	return nil
}

func runIdForObject(ni *namespaceInfo, id database.ObjectId) RunId {
	runid := ni.namespace.GetLock()

	o := IsObjectLock(runid)
	if o != nil && string(*o) == id.GetName() {
		return runid
	}
	return RunId(fmt.Sprintf("obj:%s:%s", id.GetName(), NewRunId()))
}

type Owner string

func (o Owner) Id(ns string, mm metamodel.MetaModel) database.ObjectId {
	return database.NewObjectId(mm.UpdateRequestType(), ns, string(o))
}

func IsObjectLock(runid RunId) *Owner {
	s := string(runid)

	if !strings.HasPrefix(s, "obj:") {
		return nil
	}
	s = s[4:]
	i := strings.Index(s, ":")
	if i < 0 {
		return nil
	}
	n := s[:i]
	return generics.Pointer(Owner(n))
}

func validateUpdateRequest(mm metamodel.MetaModel, action *model.UpdateAction) error {
	switch action.Action {
	case model.REQ_ACTION_ACQUIRE:
	case model.REQ_ACTION_LOCK:
	case model.REQ_ACTION_RELEASE:
	default:
		return fmt.Errorf("invalid action %q", action.Action)
	}

	for i, e := range action.Objects {
		et := mm.GetExternalType(e.GetType())
		if et == nil {
			return fmt.Errorf("invalid external type %q for element index %d", e.GetType(), i+1)
		}
		phase := mm.GetPhaseFor(e.GetType())
		if phase == nil {
			return fmt.Errorf("no trigger defined for external type %q for object index %d", e.GetType(), i+1)
		}
	}
	return nil
}
