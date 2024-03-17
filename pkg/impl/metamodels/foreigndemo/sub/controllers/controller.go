package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/expression"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/logging"
)

const FINALIZER = "expression"

type ExpressionController struct {
	pool    pool.Pool
	db      database.Database[db2.Object]
	handler *Handler
	log     logging.Logger
}

var _ pool.Action = (*ExpressionController)(nil)
var _ service.Service = (*ExpressionController)(nil)

func NewExpressionController(lctx logging.AttributionContextProvider, size int, db database.Database[db2.Object]) *ExpressionController {
	p := pool.NewPool(lctx, "controller", size, 0, true)

	c := &ExpressionController{
		pool: p,
		db:   db,
		log:  logging.DynamicLogger(logging.DefaultContext().AttributionContext().WithContext(REALM)),
	}
	return c
}

func (c *ExpressionController) Wait() error {
	return c.pool.Wait()
}

func (c *ExpressionController) Start(ctx context.Context) (service.Syncher, service.Syncher, error) {
	c.pool.AddAction(pool.ObjectType(mymetamodel.TYPE_EXPRESSION), c)
	c.handler = NewHandler(c)
	c.handler.Register()
	return c.pool.Start(ctx)
}

func (c *ExpressionController) GetTriggers() map[database.ObjectId]database.ObjectId {
	return c.handler.GetTriggers()
}

func (c *ExpressionController) Reconcile(p pool.Pool, messageContext pool.MessageContext, id database.ObjectId) pool.Status {
	log := messageContext.Logger(REALM).WithValues("expression", id)

	_o, err := c.db.GetObject(id)
	if errors.Is(err, database.ErrNotExist) {
		log.Info("skipping deleted {{expression}}")
		return pool.StatusCompleted()

	}
	if err != nil {
		return pool.StatusCompleted(err)
	}

	o := (_o).(*db.Expression)
	log.Info("reconciling {{expression}} (deleting {{deleting}} (finalizers {{finalizers}})", "deleting", _o.IsDeleting(), "finalizers", o.GetFinalizers())

	c.assureTriggers(log, o, o.Status.Generated.Objects...)
	c.assureTriggers(log, o, o.Status.Generated.Deleting...)
	c.assureTriggers(log, o, o.Status.Generated.Results...)

	if o.IsDeleting() {
		if !o.HasFinalizer(FINALIZER) {
			log.Info("finalizer already removed for deleting object -> don't touch it anymore")
			return pool.StatusCompleted()
		}
	} else {
		ok, err := db2.AddFinalizer(c.db, &o, FINALIZER)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if ok {
			log.Info("added finalizer")
		}
	}

	namespace := o.Status.Generated.Namespace

	var g graph.Graph
	var infos map[string]*ExpressionInfo
	var order []string

	values := Values{}
	v := processing.NewState(&o.Spec).GetVersion()

	if !o.IsDeleting() {
		if v == o.Status.ObservedVersion {
			log.Info("already up to date")
			return pool.StatusCompleted()
		}
		infos, order, err = Validate(o)
		if err != nil {
			return c.StatusFailed(log, o, "validation of {{expression}} failed", err)
		}

		err = PreCalc(log, order, infos, values)
		if err != nil {
			return c.StatusFailed(log, o, "validation of {{expression}} failed", err)
		}

		if m := values.Missing(infos); len(m) > 0 {
			log.Info("found external expressions {{missing}}", "missing", strings.Join(m, ","))
			if namespace == "" {
				namespace = o.GetNamespace() + "/" + o.GetName()
				_, err = database.Modify(c.db, &o, func(o *db.Expression) (bool, bool) {
					mod := support.UpdateField(&o.Status.Generated.Namespace, &namespace)
					return mod, mod
				})
				if err != nil {
					return pool.StatusCompleted(err)
				}
				log.Info("using namespace {{namespace}}", "namespace", namespace)
			}
			g, err = Generate(log, namespace, infos, values)
			if err != nil {
				return c.StatusFailed(log, o, "generation of slaves failed", err)
			}
		} else {
			g, _ = graph.NewGraph(version.Composed)
		}
	} else {
		g, _ = graph.NewGraph(version.Composed)
	}

	d := OldRefs(o, g)
	n := NewRefs(o, g)

	dr := OldResults(o, g)
	nr := NewResults(o, g)

	if len(d) > 0 || len(n) > 0 || len(dr) > 0 || len(nr) > 0 {
		if len(n) > 0 {
			log.Info("found new slaves {{new}}", "new", utils.Join(n, ","))
		}
		if len(d) > 0 {
			log.Info("found obsolete slaves {{obsolete}}", "obsolete", utils.Join(d, ","))
		}
		if len(nr) > 0 {
			log.Info("found new result {{new}}", "new", utils.Join(nr, ","))
		}
		if len(dr) > 0 {
			log.Info("found obsolete result {{obsolete}}", "obsolete", utils.Join(dr, ","))
		}
		mod := func(o *db.Expression) bool {
			m := false
			// add new generated objects and remove obsolete ones
			support.UpdateField(&o.Status.Generated.Objects, utils.Pointer(
				utils.FilterSlice(
					utils.AppendUnique(o.Status.Generated.Objects, n...),
					utils.NotFilter(utils.ContainsFilter(d...)))), &m)
			// add obsolete ones to the deletion list
			// as a result the union of generated and deleted is enriched by the new ones
			support.UpdateField(&o.Status.Generated.Deleting, utils.Pointer(utils.AppendUnique(o.Status.Generated.Deleting, d...)), &m)
			// update the new result list containing the additional triggers
			support.UpdateField(&o.Status.Generated.Results, utils.Pointer(
				utils.FilterSlice(
					utils.AppendUnique(o.Status.Generated.Results, nr...),
					utils.NotFilter(utils.ContainsFilter(dr...)))), &m)
			return m
		}
		ok, err := database.DirectModify(c.db, &o, mod)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if !ok {
			return pool.StatusCompleted(fmt.Errorf("object outdated"))
		}
		// ok, the result list is updated, now we can update the result triggers
		for _, id := range dr {
			if c.handler.Unuse(id.In(namespace)) {
				log.Info("remove result trigger for {{oid}}", "oid", id)
			}
		}
	}

	if len(o.Status.Generated.Deleting) > 0 {
		var deleted []database.LocalObjectRef

		log.Info("handle graph deletions")
		for _, l := range o.Status.Generated.Deleting {
			id := l.In(namespace)
			d, err := c.db.GetObject(id)
			if errors.Is(err, database.ErrNotExist) {
				deleted = append(deleted, l)
				log.Info("- {{oid}} already deleted", "oid", id)
				continue
			} else if err != nil {
				return pool.StatusCompleted(err)
			}

			if d.IsDeleting() {
				log.Info("- {{oid}} still deleting", "oid", id)
			} else {
				_, err := c.db.DeleteObject(id)
				if err != nil {
					return pool.StatusCompleted(err)
				}
				d, err = c.db.GetObject(id)
				if err == database.ErrNotExist {
					log.Info("- {{oid}} deleted", "oid", id)
					deleted = append(deleted, l)
				} else {
					log.Info("- {{oid}} deletion requested", "oid", id)
				}
			}
		}

		if len(deleted) > 0 {
			log.Info("removing deleted objects ({{deleted}}) from status", "deleted", utils.Join(deleted, ","))
			mod := func(o *db.Expression) bool {
				return support.UpdateField(&o.Status.Generated.Deleting, utils.Pointer(utils.FilterSlice(o.Status.Generated.Deleting, utils.NotFilter(utils.ContainsFilter(deleted...)))))
			}
			ok, err := database.DirectModify(c.db, &o, mod)
			if err != nil {
				return pool.StatusCompleted(err)
			}
			if !ok {
				return pool.StatusCompleted(fmt.Errorf("object outdated"))
			}
			for _, id := range deleted {
				src := id.In(namespace)
				if c.handler.Unuse(src) {
					log.Info("remove trigger for {{oid}}", "oid", src)
				}
			}
		}
	}

	if o.IsDeleting() {
		if len(o.Status.Generated.Deleting) == 0 && len(o.Status.Generated.Objects) == 0 {
			log.Info("no more slave objects to be deleted -> removing triggers and finalizer")
			for _, id := range o.Status.Generated.Results {
				if c.handler.Unuse(id.In(o.Status.Generated.Namespace)) {
					log.Info("remove result trigger for {{oid}}", "oid", id)
				}
			}
			_, err := db2.RemoveFinalizer(c.db, &o, FINALIZER)
			return pool.StatusCompleted(err)
		}
		log.Info("waiting for slave objects to be deleted")
		return pool.StatusCompleted()
	}

	if !g.IsEmpty() {
		for _, id := range g.Objects() {
			if c.handler.Use(id, o) {
				log.Info("establish trigger for {{oid}}", "oid", id)
			}
		}
		for _, id := range g.RootObjects() {
			if c.handler.Use(id, o) {
				log.Info("establish result trigger for {{oid}}", "oid", id)
			}
		}

		mod, err := g.UpdateDB(log, c.db)
		if mod || err != nil {
			if err != nil {
				log.LogError(err, "db update for generated object failed")
			}
			if mod {
				log.Info("generated graph updated on db -> skip further processing")
			}
			return pool.StatusCompleted(err)
		}

		final, status, err := g.CheckDB(log, c.db)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if !final {
			log.Info("expression graph processing not yet final ({{status}})", "status", status)
			return pool.StatusCompleted()
		}
		if status != model.STATUS_COMPLETED {
			return c.StatusFailed(log, o, "", fmt.Errorf("graph processing failed"))
		}
	} else {
		log.Info("no expression graph required")
	}

	if !values.IsComplete(infos) {
		err = Gather(log, c.db, namespace, infos, values)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		err = PreCalc(log, order, infos, values)
		if err != nil {
			return c.StatusFailed(log, o, "expression calculation failed", err)
		}
	}

	out := db.ExpressionOutput{}

	log.Info("setting outputs")
	for _, n := range utils.OrderedMapKeys(o.Spec.Expressions) {
		out[n] = values[n]
		log.Info("- {{output}} = {{value}}", "output", n, "value", out[n])
	}

	l := len(o.Spec.Expressions)
	log.Info("operation completed for version {{version}}", "version", v)
	mod := func(o *db.Expression) (bool, bool) {
		mod := false
		support.UpdateField(&o.Status.Status, utils.Pointer(model.STATUS_COMPLETED), &mod)
		support.UpdateField(&o.Status.Message, utils.Pointer(fmt.Sprintf("%d expressions calculated", l)), &mod)
		support.UpdateField(&o.Status.ObservedVersion, &v, &mod)

		support.UpdateField(&o.Status.Output, &out, &mod)
		return mod, mod
	}
	_, err = database.Modify(c.db, &o, mod)
	return pool.StatusCompleted(err)
}

func (c *ExpressionController) Command(p pool.Pool, messageContext pool.MessageContext, command pool.Command) pool.Status {
	return pool.StatusCompleted()
}

func (c *ExpressionController) StatusFailed(log logging.Logger, o *db.Expression, msg string, err error) pool.Status {
	v := o.Spec.GetVersion()

	log.LogError(err, "operation failed ({{message}}) for observed version {{version}}", "message", msg, "version", v)
	mod := func(o *db.Expression) (bool, bool) {
		mod := false
		support.UpdateField(&o.Status.Status, utils.Pointer(model.STATUS_FAILED), &mod)
		support.UpdateField(&o.Status.Message, utils.Pointer(err.Error()), &mod)
		support.UpdateField(&o.Status.ObservedVersion, &v, &mod)
		return mod, mod
	}

	_, uerr := database.Modify(c.db, &o, mod)
	if uerr != nil {
		pool.StatusCompleted(uerr)
	}
	return pool.StatusFailed(err)
}

func (c *ExpressionController) assureTriggers(log logging.Logger, o *db.Expression, list ...database.LocalObjectRef) {
	for _, id := range list {
		if c.handler.Use(id.In(o.Status.Generated.Namespace), o) {
			log.Info("re-establish trigger for {{oid}}", "oid", id)
		}
	}
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
