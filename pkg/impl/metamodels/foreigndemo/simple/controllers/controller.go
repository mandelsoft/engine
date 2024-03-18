package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/simple/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

type ExpressionController struct {
	lctx   logging.AttributionContextProvider
	worker int

	pool pool.Pool
	db   database.Database[db2.Object]
}

var _ pool.Action = (*ExpressionController)(nil)
var _ service.Service = (*ExpressionController)(nil)

func NewExpressionController(lctx logging.AttributionContextProvider, size int, db database.Database[db2.Object]) *ExpressionController {
	c := &ExpressionController{
		pool: pool.NewPool(lctx, "controller", size, 0),
		db:   db,
	}
	return c
}

func (c *ExpressionController) Wait() error {
	return c.pool.Wait()
}

func (c *ExpressionController) Start(ctx context.Context) (service.Syncher, service.Syncher, error) {
	c.pool.AddAction(pool.ObjectType(mymetamodel.TYPE_EXPRESSION), c)
	h := &Handler{c}
	c.db.RegisterHandler(h, true, mymetamodel.TYPE_EXPRESSION, true, "")
	return c.pool.Start(ctx)
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
	v := processing.NewState(&o.Spec).GetVersion()

	if v == o.Status.ObservedVersion {
		log.Info("already up to date")
		return pool.StatusCompleted()
	}
	err = Validate(o)
	if err != nil {
		return c.StatusFailed(log, o, "validation of {{expression}} failed", err)
	}

	out, err := Calculate(log, o)
	if err != nil {
		return c.StatusFailed(log, o, "calculation of {{expression}} failed", err)
	}

	l := len(o.Spec.Expressions)
	log.Info("operation completed for version {{version}}", "version", v)
	mod := func(o *db.Expression) (bool, bool) {
		mod := false
		support.UpdateField(&o.Status.Status, generics.Pointer(model.STATUS_COMPLETED), &mod)
		support.UpdateField(&o.Status.Message, generics.Pointer(fmt.Sprintf("%d expressions calculated", l)), &mod)
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
		support.UpdateField(&o.Status.Status, generics.Pointer(model.STATUS_FAILED), &mod)
		support.UpdateField(&o.Status.Message, generics.Pointer(err.Error()), &mod)
		support.UpdateField(&o.Status.ObservedVersion, &v, &mod)
		return mod, mod
	}

	_, uerr := database.Modify(c.db, &o, mod)
	if uerr != nil {
		pool.StatusCompleted(uerr)
	}
	return pool.StatusFailed(err)
}

func Validate(o *db.Expression) error {
	if len(o.Spec.Operands) == 0 && len(o.Spec.Expressions) > 0 {
		return fmt.Errorf("no operand specified")
	}
	for n, e := range o.Spec.Expressions {
		switch e.Operator {
		case db.OP_ADD, db.OP_SUB, db.OP_MUL, db.OP_DIV:
			for _, a := range e.Operands {
				if _, ok := o.Spec.Operands[a]; !ok {
					return fmt.Errorf("operand %q for expression %q not found", a, n)
				}
			}
		default:
			return fmt.Errorf("invalid operator %q for expression %q", e.Operator, n)
		}
	}
	return nil
}

func Calculate(log logging.Logger, o *db.Expression) (db.ExpressionOutput, error) {
	out := db.ExpressionOutput{}

	if len(o.Spec.Expressions) == 0 {
		log.Info("no expressions found")
	}
	for n, e := range o.Spec.Expressions {
		var operands []int
		for _, a := range e.Operands {
			operands = append(operands, o.Spec.Operands[a])
		}
		op := e.Operator

		r := operands[0]
		log.Info("calculate operation {{operation}}: {{operator}} {{operands}}", "operation", n, "operator", op, "operands", operands)
		switch op {
		case db.OP_ADD:
			for _, v := range operands[1:] {
				r += v
			}
		case db.OP_SUB:
			for _, v := range operands[1:] {
				r -= v
			}
		case db.OP_MUL:
			for _, v := range operands[1:] {
				r *= v
			}
		case db.OP_DIV:
			for _, v := range operands[1:] {
				if v == 0 {
					return nil, fmt.Errorf("division by zero for operation %q", n)
				}
				r /= v
			}
		}
		out[n] = r
		log.Info("result {{result}}", "result", r)
	}
	return out, nil
}
