package processor

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/stringutils"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

type updaterequestReconciler struct {
	reconciler

	lock   sync.Mutex
	status map[database.ObjectId]*model.UpdateStatus
}

var _ pool.Action = (*updaterequestReconciler)(nil)

func newUpdateRequestReconciler(c *Controller) *updaterequestReconciler {
	return &updaterequestReconciler{
		reconciler: reconciler{controller: c},
		status:     map[database.ObjectId]*model.UpdateStatus{},
	}
}

// cache request status to avoid race condition when reading
// own object status in reconcilation.

func (r *updaterequestReconciler) getStatus(ur model.UpdateRequestObject) *model.UpdateStatus {
	r.lock.Lock()
	defer r.lock.Unlock()

	s := r.status[database.NewObjectIdFor(ur)]
	if s != nil {
		return s
	}
	return ur.GetStatus()
}

func (r *updaterequestReconciler) setStatus(ur model.UpdateRequestObject, status *model.UpdateStatus) (bool, error) {
	mod, err := ur.SetStatus(r.processingModel.ObjectBase(), status)
	if err != nil {
		return mod, err
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	r.status[database.NewObjectIdFor(ur)] = status
	return mod, nil
}

func (r *updaterequestReconciler) deleteStatus(oid database.ObjectId) {
	r.lock.Lock()
	defer r.lock.Unlock()

	delete(r.status, database.NewObjectIdFor(oid))
}

func (r *updaterequestReconciler) Reconcile(_ pool.Pool, ctx pool.MessageContext, id database.ObjectId) pool.Status {
	return newUpdateRequestReconcilation(r, ctx, id).Reconcile()
}

type updaterequestReconcilation struct {
	*updaterequestReconciler
	logging.Logger
	model.UpdateRequestObject
	lctx model.Logging

	oid database.ObjectId
	ni  *namespaceInfo
}

var _ Reconcilation = (*updaterequestReconcilation)(nil)

func newUpdateRequestReconcilation(r *updaterequestReconciler, ctx pool.MessageContext, id database.ObjectId) *updaterequestReconcilation {
	oid := NewObjectIdFor(id)

	return &updaterequestReconcilation{
		updaterequestReconciler: r,
		lctx:                    ctx,
		oid:                     oid,
		Logger:                  r.logging.Logger(logging.ExcludeFromMessageContext[logging.Realm](ctx)).WithValues("reqid", oid),
	}
}

func (r *updaterequestReconcilation) LoggingContext() model.Logging {
	return r.lctx
}

func (r *updaterequestReconcilation) Reconcile() pool.Status {
	tmp := r.processingModel.GetNamespace(r.oid.GetNamespace())
	if tmp != nil {
		r.ni = tmp.(*namespaceInfo)
		r.ni.lock.Lock()
		defer r.ni.lock.Unlock()
	}

	action, err := r.prepareAction()
	if action == nil || err != nil {
		return pool.StatusCompleted(err)
	}

	if err := r.validateUpdateRequest(action); err != nil {
		r.Error("invalid request: {{message}}", "message", err.Error())
		suberr := r.clearNamespaceLockForObject()
		if suberr != nil {
			return pool.StatusCompleted(suberr)
		}
		mod, suberr := r.setStatus(model.REQ_STATUS_INVALID, err.Error())
		if mod {
			r.Info("updated status to {{status}}: {{message}}", "status", model.REQ_STATUS_INVALID, "message", err.Error())
		}
		return pool.StatusCompleted(suberr)
	}

	r.ni, err = r.processingModel.assureNamespace(r.Logger, r.GetNamespace(), true)
	if err != nil {
		return pool.StatusCompleted(err)
	}

	// step 1: assure namespace locked
	runid := r.runIdForObject()
	r.Logger = r.WithValues("runid", runid)
	r.Info("step 1: trying to acquire namespace lock for request {{reqid}}: {{runid}}")

	ok, err := r.ni.tryLock(r, runid)
	if err != nil {
		return pool.StatusCompleted(err)
	}
	if !ok {
		return pool.StatusCompleted(fmt.Errorf("retry aquiring namespace lock"))
	}
	r.Info("  acquired namespace lock {{runid}}")
	switch r.getStatus().Status {
	case model.REQ_STATUS_RELEASED, "":
		_, err := r.setStatus(model.REQ_STATUS_ACQUIRED, "namespace locked")
		if err != nil {
			return pool.StatusCompleted(err)
		}
	}

	// step 2: lock elements
	switch action.Action {
	case model.REQ_ACTION_LOCK, model.REQ_ACTION_RELEASE:
		var elems []_Element

		for _, es := range action.Objects {
			tid := r.processingModel.MetaModel().GetPhaseFor(es.Type)
			eid := NewElementIdForType(*tid, r.GetNamespace(), es.Name)
			e := r.ni._getElement(eid)
			if e != nil {
				elems = append(elems, e)
			}
		}
		r.Info("step 2: locking elements for {{runid}}; {{elements}}", "elements", stringutils.Join(action.Objects, ", "))
		ok, err := r.ni.doLockGraph(r, runid, true, elems...)
		if err != nil {
			if ok {
				r.Info("  elements not yet completely lockable")
				_, err = r.setStatus(model.REQ_STATUS_PENDING, "waiting for lockable elements")
				if err != nil {
					return pool.StatusCompleted(err)
				}
			}
			return pool.StatusCompleted(err)
		}
		_, err = r.setStatus(model.REQ_STATUS_LOCKED, "elements locked")
		if err != nil {
			return pool.StatusCompleted(err)
		}
		r.Info("  elements locked {{elements}}", "elements", stringutils.Join(action.Objects, ", "))
	}

	// step 3: release locks
	switch action.Action {
	case model.REQ_ACTION_RELEASE:
		r.Info("step 3: releasing namespace lock {{runid}}")
		for _, es := range action.Objects {
			tid := r.processingModel.MetaModel().GetPhaseFor(es.Type)
			eid := NewElementIdForType(*tid, r.GetNamespace(), es.Name)
			oid := es.In(r.GetNamespace())
			e := r.ni._getElement(eid)
			if e == nil {
				_, err := r.processingModel.ObjectBase().GetObject(oid)
				if err != nil {
					if !errors.Is(err, database.ErrNotExist) {
						return pool.StatusCompleted(err)
					}
					r.Info("- object {{oid}} does not exist", "oid", oid)
					_, err = r.setStatus(model.REQ_STATUS_INVALID, fmt.Sprintf("object %q does not exist", oid))
					if err != nil {
						return pool.StatusCompleted(err)
					}
					return pool.StatusCompleted(r.clearNamespaceLockForObject())
				}
				r.Info("- object {{oid}} exists, but is not yet reconciled -> dely", "oid", oid)
				return pool.StatusCompleted(fmt.Errorf("waiting for object %q to be reconciled", oid))
			}
			r.Info("- object {{oid}} ready for release", "oid", oid)
		}

		r.Info("all objects ready: releasing namespace lock {{runid}}")
		err := r.clearNamespaceLockForObject()
		if err != nil {
			return pool.StatusCompleted(err)
		}

		_, err = r.setStatus(model.REQ_STATUS_RELEASED, "elements locked and namespace released")
		if err != nil {
			return pool.StatusCompleted(err)
		}
		r.Info("  released namespace lock {{runid}}")
	}

	return pool.StatusCompleted()
}

func (r *updaterequestReconcilation) prepareAction() (*model.UpdateAction, error) {
	_o, err := r.processingModel.ObjectBase().GetObject(r.oid)
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return nil, err
		}
		r.Info("request deleted")
		r.deleteStatus(r.oid)
		// object deleted -> unlock
		if r.ni == nil {
			return nil, nil
		}
		return nil, r.clearNamespaceLockForObject()
	}
	r.UpdateRequestObject = _o.(model.UpdateRequestObject)

	status := r.getStatus() // get actual status known to reconciler
	action := r.GetAction()
	if action.Action == model.REQ_ACTION_RELEASE &&
		(status.Status == model.REQ_STATUS_RELEASED || status.Status == model.REQ_STATUS_INVALID) {
		r.Info("request already done")
		return nil, r.clearNamespaceLockForObject()
	}
	return action, nil
}

func (r *updaterequestReconcilation) getStatus() *model.UpdateStatus {
	return r.updaterequestReconciler.getStatus(r.UpdateRequestObject)
}

func (r *updaterequestReconcilation) setStatus(status string, message string) (bool, error) {
	snew := *r.getStatus()
	snew.Status = status
	snew.Message = message
	snew.ObservedVersion = r.GetAction().Version()
	return r.updaterequestReconciler.setStatus(r.UpdateRequestObject, &snew)
}

func (r *updaterequestReconcilation) clearNamespaceLockForObject() error {
	if r.ni == nil {
		return nil
	}
	runid := r.ni.namespace.GetLock()
	owner := IsObjectLock(runid)
	if owner != nil && database.CompareObject((*owner).Id(r.GetNamespace(), r.processingModel.mm), r.oid) == 0 {
		r.Info("clear namespace lock {{runid}}", "runid", runid)
		err := r.ni.clearLock(r, runid)
		if err != nil {
			return err
		}
		r.Info("trigger elements for runid {{runid}}", "runid", runid)
		for eid := range r.ni.filterElements(func(e _Element) bool { return e.GetLock() == runid }) {
			r.Info("- trigger element {{eid}}", "eid", eid)
			r.EnqueueKey(CMD_ELEM, eid)
		}
	}
	return nil
}

func (r *updaterequestReconcilation) validateUpdateRequest(action *model.UpdateAction) error {
	switch action.Action {
	case model.REQ_ACTION_ACQUIRE:
	case model.REQ_ACTION_LOCK:
	case model.REQ_ACTION_RELEASE:
	default:
		return fmt.Errorf("invalid action %q", action.Action)
	}

	for i, e := range action.Objects {
		et := r.processingModel.MetaModel().GetExternalType(e.GetType())
		if et == nil {
			return fmt.Errorf("invalid external type %q for element index %d", e.GetType(), i+1)
		}
		phase := r.processingModel.MetaModel().GetPhaseFor(e.GetType())
		if phase == nil {
			return fmt.Errorf("no trigger defined for external type %q for object index %d", e.GetType(), i+1)
		}
	}
	return nil
}

func (r *updaterequestReconcilation) runIdForObject() RunId {
	runid := r.ni.namespace.GetLock()

	o := IsObjectLock(runid)
	if o != nil && string(*o) == r.GetName() {
		return runid
	}
	return RunId(fmt.Sprintf("obj:%s:%s", r.GetName(), NewRunId()))
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
