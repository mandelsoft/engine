package controllers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/expression"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/matcher"
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/goutils/stringutils"
	"github.com/mandelsoft/logging"
)

type reconciler struct {
	pool.DefaultAction
	*ExpressionController
}

func newReconciler(c *ExpressionController) *reconciler {
	return &reconciler{ExpressionController: c}
}

func (c *reconciler) Reconcile(_ pool.Pool, messageContext pool.MessageContext, id database.ObjectId) pool.Status {
	log := messageContext.Logger(REALM)
	log = log.WithValues("expression", id)

	_o, err := c.db.GetObject(id)
	if errors.Is(err, database.ErrNotExist) {
		log.Info("skipping deleted {{expression}}")
		return pool.StatusCompleted()

	}
	if err != nil {
		return pool.StatusCompleted(err)
	}
	return newReconcilation(c, log, _o).Reconcile()
}

type obj = db.Expression
type reconcilation struct {
	*reconciler
	logging.Logger
	*obj

	namespace string
}

func newReconcilation(r *reconciler, log logging.Logger, _o db2.Object) *reconcilation {
	o := _o.(*db.Expression)
	return &reconcilation{
		reconciler: r,
		Logger:     log,
		obj:        o,
		namespace:  o.Status.Generated.Namespace,
	}
}

func (c *reconcilation) Reconcile() pool.Status {
	c.Info("reconciling {{expression}} (deleting {{deleting}} (finalizers {{finalizers}})", "deleting", c.IsDeleting(), "finalizers", c.GetFinalizers())

	c.assureTriggers(c.Status.Generated.Objects...)
	c.assureTriggers(c.Status.Generated.Deleting...)
	c.assureTriggers(c.Status.Generated.Results...)

	if c.IsDeleting() {
		if !c.HasFinalizer(FINALIZER) {
			c.Info("finalizer already removed for deleting object -> don't touch it anymore")
			return pool.StatusCompleted()
		}
	} else {
		ok, err := db2.AddFinalizer(c.db, &c.obj, FINALIZER)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if ok {
			c.Info("added finalizer")
		}
	}

	var err error
	var g graph.Graph
	var infos map[string]*ExpressionInfo
	var order []string

	values := Values{}
	v := processing.NewState(&c.Spec).GetVersion()

	if !c.IsDeleting() {
		if v == c.Status.ObservedVersion {
			c.Info("already up to date")
			return pool.StatusCompleted()
		}
		infos, order, err = Validate(c.obj)
		if err != nil {
			return c.StatusFailed("validation of {{expression}} failed", err)
		}

		err = PreCalc(c.Logger, order, infos, values)
		if err != nil {
			return c.StatusFailed("validation of {{expression}} failed", err)
		}

		if m := values.Missing(infos); len(m) > 0 {
			c.Info("found external expressions {{missing}}", "missing", strings.Join(m, ","))
			if err = c.assureSubNamespace(); err != nil {
				return pool.StatusCompleted(err)
			}
			g, err = Generate(c.Logger, c.namespace, infos, values)
			if err != nil {
				return c.StatusFailed("generation of slaves failed", err)
			}
		} else {
			g, _ = graph.NewGraph(version.Composed)
		}
	} else {
		g, _ = graph.NewGraph(version.Composed)
	}
	d := OldRefs(c.obj, g)
	n := NewRefs(c.obj, g)

	dr := OldResults(c.obj, g)
	nr := NewResults(c.obj, g)

	if len(d) > 0 || len(n) > 0 || len(dr) > 0 || len(nr) > 0 {
		if len(n) > 0 {
			c.Info("found new slaves {{new}}", "new", stringutils.Join(n, ","))
		}
		if len(d) > 0 {
			c.Info("found obsolete slaves {{obsolete}}", "obsolete", stringutils.Join(d, ","))
		}
		if len(nr) > 0 {
			c.Info("found new result {{new}}", "new", stringutils.Join(nr, ","))
		}
		if len(dr) > 0 {
			c.Info("found obsolete result {{obsolete}}", "obsolete", stringutils.Join(dr, ","))
		}
		mod := func(o *db.Expression) bool {
			m := false
			// add new generated objects and remove obsolete ones
			support.UpdateField(&o.Status.Generated.Objects, generics.Pointer(
				sliceutils.Filter(
					sliceutils.AppendUnique(o.Status.Generated.Objects, n...),
					matcher.Not(matcher.Contains(d...)))), &m)
			// add obsolete ones to the deletion list
			// as a result the union of generated and deleted is enriched by the new ones
			support.UpdateField(&o.Status.Generated.Deleting, generics.Pointer(sliceutils.AppendUnique(o.Status.Generated.Deleting, d...)), &m)
			// update the new result list containing the additional triggers
			support.UpdateField(&o.Status.Generated.Results, generics.Pointer(
				sliceutils.Filter(
					sliceutils.AppendUnique(o.Status.Generated.Results, nr...),
					matcher.Not(matcher.Contains(dr...)))), &m)
			return m
		}
		ok, err := database.DirectModify(c.db, &c.obj, mod)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if !ok {
			return pool.StatusCompleted(fmt.Errorf("object outdated"))
		}
		// ok, the result list is updated, now we can update the result triggers
		for _, id := range dr {
			if c.handler.Unuse(id.In(c.namespace)) {
				c.Info("remove result trigger for {{oid}}", "oid", id)
			}
		}
	}

	if len(c.Status.Generated.Deleting) > 0 {
		var deleted []database.LocalObjectRef

		c.Info("handle graph deletions")
		for _, l := range c.Status.Generated.Deleting {
			id := l.In(c.namespace)
			d, err := c.db.GetObject(id)
			if errors.Is(err, database.ErrNotExist) {
				deleted = append(deleted, l)
				c.Info("- {{oid}} already deleted", "oid", id)
				continue
			} else if err != nil {
				return pool.StatusCompleted(err)
			}

			if d.IsDeleting() {
				c.Info("- {{oid}} still deleting", "oid", id)
			} else {
				_, err := c.db.DeleteObject(id)
				if err != nil {
					return pool.StatusCompleted(err)
				}
				d, err = c.db.GetObject(id)
				if err == database.ErrNotExist {
					c.Info("- {{oid}} deleted", "oid", id)
					deleted = append(deleted, l)
				} else {
					c.Info("- {{oid}} deletion requested", "oid", id)
				}
			}
		}

		if len(deleted) > 0 {
			c.Info("removing deleted objects ({{deleted}}) from status", "deleted", stringutils.Join(deleted, ","))
			mod := func(o *db.Expression) bool {
				return support.UpdateField(&o.Status.Generated.Deleting, generics.Pointer(sliceutils.Filter(o.Status.Generated.Deleting, matcher.Not(matcher.Contains(deleted...)))))
			}
			ok, err := database.DirectModify(c.db, &c.obj, mod)
			if err != nil {
				return pool.StatusCompleted(err)
			}
			if !ok {
				return pool.StatusCompleted(fmt.Errorf("object outdated"))
			}
			for _, id := range deleted {
				src := id.In(c.namespace)
				if c.handler.Unuse(src) {
					c.Info("remove trigger for {{oid}}", "oid", src)
				}
			}
		}
	}

	if c.IsDeleting() {
		if len(c.Status.Generated.Deleting) == 0 && len(c.Status.Generated.Objects) == 0 {
			_, err := c.db.DeleteObject(database.NewObjectId(mymetamodel.TYPE_UPDATEREQUEST, c.namespace, c.GetName()))
			if err != nil && !errors.Is(err, database.ErrNotExist) {
				return pool.StatusCompleted(err)
			}
			c.Info("no more slave objects to be deleted -> removing triggers and finalizer")
			for _, id := range c.Status.Generated.Results {
				if c.handler.Unuse(id.In(c.Status.Generated.Namespace)) {
					c.Info("remove result trigger for {{oid}}", "oid", id)
				}
			}
			_, err = db2.RemoveFinalizer(c.db, &c.obj, FINALIZER)
			return pool.StatusCompleted(err)
		}
		c.Info("waiting for slave objects to be deleted")
		return pool.StatusCompleted()
	}

	if !g.IsEmpty() {
		for _, id := range g.Objects() {
			if c.handler.Use(id, c) {
				c.Info("establish trigger for {{oid}}", "oid", id)
			}
		}
		for _, id := range g.RootObjects() {
			if c.handler.Use(id, c) {
				c.Info("establish result trigger for {{oid}}", "oid", id)
			}
		}

		changes, err := g.IsModifiedDB(c.Logger, c.db)
		if err != nil {
			return pool.StatusCompleted(err)
		}

		if len(changes) > 0 {
			locked, err := c.requestUpdate(c.Logger, changes, c.namespace, c.GetName())
			if err != nil || !locked {
				return pool.StatusCompleted(err)
			}

			mod, err := g.UpdateDB(c.Logger, c.db)
			if mod || err != nil {
				if err != nil {
					c.LogError(err, "db update for generated object failed")
				}
				if mod {
					c.Info("generated graph updated on db -> skip further processing")
				}
				return pool.StatusCompleted(err)
			}
		}

		ur := db.NewUpdateRequest(c.namespace, c.GetName()).RequestAction(model.REQ_ACTION_RELEASE)
		_, err = database.ModifyExisting(c.db, &ur, func(o *db.UpdateRequest) bool {
			mod := false
			if support.UpdateField(&o.Spec.Action, generics.Pointer(model.REQ_ACTION_RELEASE), &mod) {
				c.Info(" update action:  release")
			}
			return mod
		})

		final, status, err := g.CheckDB(c.Logger, c.db)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if !final {
			c.Info("expression graph processing not yet final ({{status}})", "status", status)
			return pool.StatusCompleted()
		}
		if status != model.STATUS_COMPLETED {
			return c.StatusFailed("", fmt.Errorf("graph processing failed"))
		}
	} else {
		c.Info("no expression graph required")
	}

	if !values.IsComplete(infos) {
		err = Gather(c.Logger, c.db, c.namespace, infos, values)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		err = PreCalc(c.Logger, order, infos, values)
		if err != nil {
			return c.StatusFailed("expression calculation failed", err)
		}
	}

	out := db.ExpressionOutput{}

	c.Info("setting outputs")
	for _, n := range maputils.OrderedKeys(c.Spec.Expressions) {
		out[n] = values[n]
		c.Info("- {{output}} = {{value}}", "output", n, "value", out[n])
	}

	l := len(c.Spec.Expressions)
	c.Info("operation completed for version {{version}}", "version", v)
	mod := func(o *db.Expression) (bool, bool) {
		mod := false
		support.UpdateField(&o.Status.Status, generics.Pointer(model.STATUS_COMPLETED), &mod)
		support.UpdateField(&o.Status.Message, generics.Pointer(fmt.Sprintf("%d expressions calculated", l)), &mod)
		support.UpdateField(&o.Status.ObservedVersion, &v, &mod)

		support.UpdateField(&o.Status.Output, &out, &mod)
		return mod, mod
	}
	_, err = database.Modify(c.db, &c.obj, mod)
	return pool.StatusCompleted(err)
}

func (c *reconcilation) assureSubNamespace() error {
	if c.namespace == "" {
		c.namespace = c.GetNamespace() + "/" + c.GetName()
		_, err := database.Modify(c.db, &c.obj, func(o *db.Expression) (bool, bool) {
			mod := support.UpdateField(&o.Status.Generated.Namespace, &c.namespace)
			return mod, mod
		})
		if err != nil {
			return err
		}
		c.Info("using namespace {{namespace}}", "namespace", c.namespace)
	}
	return nil
}

func (c *reconcilation) StatusFailed(msg string, err error) pool.Status {
	v := c.Spec.GetVersion()

	c.LogError(err, "operation failed ({{message}}) for observed version {{version}}", "message", msg, "version", v)
	mod := func(o *db.Expression) (bool, bool) {
		mod := false
		support.UpdateField(&o.Status.Status, generics.Pointer(model.STATUS_FAILED), &mod)
		support.UpdateField(&o.Status.Message, generics.Pointer(err.Error()), &mod)
		support.UpdateField(&o.Status.ObservedVersion, &v, &mod)
		return mod, mod
	}

	_, uerr := database.Modify(c.db, &c.obj, mod)
	if uerr != nil {
		pool.StatusCompleted(uerr)
	}
	return pool.StatusFailed(err)
}

func (c *reconcilation) assureTriggers(list ...database.LocalObjectRef) {
	for _, id := range list {
		if c.handler.Use(id.In(c.Status.Generated.Namespace), c.obj) {
			c.Info("re-establish trigger for {{oid}}", "oid", id)
		}
	}
}

func (c *reconcilation) requestUpdate(log logging.Logger, changes []database.LocalObjectRef, namespace, name string) (bool, error) {
	if !c.sync {
		return true, nil
	}

	log.Info("initiate change request for {{elements}}", "elements", stringutils.Join(changes, ", "))
	var ur *db.UpdateRequest
	uo, err := c.db.GetObject(database.NewObjectId(mymetamodel.TYPE_UPDATEREQUEST, namespace, name))
	if err != nil {
		if !errors.Is(err, database.ErrNotExist) {
			return false, err
		}
		ur = db.NewUpdateRequest(namespace, name).RequestAction(model.REQ_ACTION_ACQUIRE)
	} else {
		ur = uo.(*db.UpdateRequest)
	}
	locked := false
	_, err = database.CreateOrModify(c.db, &ur, func(o *db.UpdateRequest) bool {
		mod := false
		switch o.Status.Status {
		case model.REQ_STATUS_RELEASED, model.REQ_STATUS_INVALID:
			if support.UpdateField(&o.Spec.Action, generics.Pointer(model.REQ_ACTION_ACQUIRE), &mod) {
				log.Info(" update action:  acquire")
			}
		case model.REQ_STATUS_ACQUIRED, model.REQ_STATUS_PENDING:
			if support.UpdateField(&o.Spec.Action, generics.Pointer(model.REQ_ACTION_LOCK), &mod) {
				log.Info(" update action:  lock")
			}
			if support.UpdateField(&o.Spec.Objects, generics.Pointer(changes), &mod) {
				log.Info("  update objects: {{elements}}", "elements", stringutils.Join(changes, ", "))
			}
		case model.REQ_STATUS_LOCKED:
			if support.UpdateField(&o.Spec.Objects, generics.Pointer(changes), &mod) {
				log.Info("  update elements: {{elements)", "elements", stringutils.Join(changes, ", "))
			} else {
				locked = o.GetAction().Version() == o.GetStatus().ObservedVersion
				if locked {
					log.Info("  modification list finally locked")
				} else {
					log.Info("  modification list locked, but not up-to-date")
				}
			}
		}
		return mod
	})
	log.Info("update status: {{status}}", "status", ur.Status.Status)
	return locked, err
}

type ExpressionInfo struct {
	Value    *int
	Operator db.OperatorName
	Node     *expression.Node
	Operands []string
}

func NewExpressionInfo(ops ...string) *ExpressionInfo {
	return &ExpressionInfo{
		Operands: ops,
	}
}

func (i *ExpressionInfo) String() string {
	if i.Value != nil {
		return fmt.Sprintf("%d", *i.Value)
	}
	if i.Node != nil {
		return i.Node.String()
	}
	return fmt.Sprintf("%s [%s]", i.Operator, strings.Join(i.Operands, ","))
}
