package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/expression"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/logging"
)

type ExpressionController struct {
	pool pool.Pool
	db   database.Database[db2.DBObject]
}

var _ pool.Action = (*ExpressionController)(nil)

func NewExpressionController(ctx context.Context, lctx logging.AttributionContextProvider, size int, db database.Database[db2.DBObject]) *ExpressionController {
	p := pool.NewPool(ctx, lctx, "controller", size, 0, true)

	c := &ExpressionController{
		pool: p,
		db:   db,
	}
	return c
}

func (c *ExpressionController) Start(wg *sync.WaitGroup) error {
	c.pool.AddAction(pool.ObjectType(mymetamodel.TYPE_EXPRESSION), c)

	h := &Handler{c}
	c.db.RegisterHandler(h, true, mymetamodel.TYPE_EXPRESSION)
	c.pool.Start(wg)
	return nil
}

func (c *ExpressionController) Reconcile(p pool.Pool, messageContext pool.MessageContext, id database.ObjectId) pool.Status {
	log := messageContext.Logger(REALM).WithValues("expression", id)
	log.Info("reconciling {{expression}}")

	_o, err := c.db.GetObject(id)
	if errors.Is(err, database.ErrNotExist) {
		log.Info("{{expression}} deleted")
		return pool.StatusCompleted()

	}
	if err != nil {
		return pool.StatusCompleted(err)
	}

	o := (_o).(*db.Expression)
	v := support.NewState(&o.Spec).GetVersion()

	if v == o.Status.ObservedVersion {
		log.Info("already up to date")
		return pool.StatusCompleted()
	}
	infos, order, err := Validate(o)
	if err != nil {
		return c.StatusFailed(log, o, "validation of {{expression}} failed", err)
	}

	values := Values{}
	err = PreCalc(log, order, infos, values)

	if err != nil {
		return c.StatusFailed(log, o, "validation of {{expression}} failed", err)
	}

	namespace := o.Status.Generated.Namespace

	var g graph.Graph
	if m := values.Missing(infos); len(m) > 0 {
		log.Info("found external expressions {{missing}}", "missing", strings.Join(m, ","))
		if namespace == "" {
			namespace = o.Namespace + "/" + o.Name
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

	d := OldRefs(o, g)
	n := NewRefs(o, g)
	if len(d) > 0 || len(n) > 0 {
		log.Info("found new slaves {{new}}", "new", utils.Join(n, ","))
		log.Info("found obsolete slaves {{obsolete}}", "obsolete", utils.Join(d, ","))
		mod := func(o *db.Expression) bool {
			m := false
			support.UpdateField(&o.Status.Generated.Objects, utils.Pointer(utils.AppendUnique(o.Status.Generated.Objects, n...)), &m)
			support.UpdateField(&o.Status.Generated.Deleting, utils.Pointer(utils.AppendUnique(o.Status.Generated.Deleting, d...)), &m)
			return m
		}
		ok, err := database.DirectModify(c.db, &o, mod)
		if err != nil {
			return pool.StatusCompleted(err)
		}
		if !ok {
			return pool.StatusCompleted(fmt.Errorf("object outdated"))
		}
	}

	if len(o.Status.Generated.Deleting) > 0 {
		var deleted []database.LocalObjectRef

		log.Info("handled graph deletions")
		for _, l := range o.Status.Generated.Deleting {
			id := l.In(namespace)
			d, err := c.db.GetObject(id)
			if err == database.ErrNotExist {
				deleted = append(deleted, l)
				log.Info("- {{oid}} already deleted", "oid", id)
			} else if err != nil {
				return pool.StatusCompleted(err)
			}

			if d.IsDeleting() {
				log.Info("- {{oid}} still deleting", "oid", id)
			} else {
				err := c.db.DeleteObject(id)
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
			log.Info("removing deleted objects ({{deleted}})", utils.Join(deleted, ","))
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
		}
	}

	if !g.IsEmpty() {
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

type Handler struct {
	c *ExpressionController
}

func (h *Handler) HandleEvent(id database.ObjectId) {
	h.c.pool.EnqueueKey(id)
}

var _ database.EventHandler = (*Handler)(nil)

func (c *ExpressionController) StatusFailed(log logging.Logger, o *db.Expression, msg string, err error) pool.Status {
	v := o.Spec.GetVersion()

	log.LogError(err, "operation failed ({{msg}}) for observed version {{version}}", "message", msg, "version", v)
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
