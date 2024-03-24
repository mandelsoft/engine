package controllers

import (
	"context"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/pool"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/logging"
)

const FINALIZER = "expression"

type ExpressionController struct {
	pool    pool.Pool
	sync    bool
	db      database.Database[db2.Object]
	handler *Handler
	log     logging.Logger
}

var _ pool.Action = (*reconciler)(nil)
var _ service.Service = (*ExpressionController)(nil)

func NewExpressionController(lctx logging.AttributionContextProvider, size int, db database.Database[db2.Object]) *ExpressionController {
	p := pool.NewPool(lctx, "controller", size, 0, true)

	c := &ExpressionController{
		pool: p,
		db:   db,
		sync: true,
		log:  logging.DynamicLogger(logging.DefaultContext().AttributionContext().WithContext(REALM)),
	}
	return c
}

func (c *ExpressionController) SetSyncMode(b bool) {
	c.sync = b
}

func (c *ExpressionController) Wait() error {
	return c.pool.Wait()
}

func (c *ExpressionController) Start(ctx context.Context) (service.Syncher, service.Syncher, error) {
	c.pool.AddAction(pool.ObjectType(mymetamodel.TYPE_EXPRESSION), newReconciler(c))
	c.handler = NewHandler(c)
	c.handler.Register()
	return c.pool.Start(ctx)
}

func (c *ExpressionController) GetTriggers() map[database.ObjectId]database.ObjectId {
	return c.handler.GetTriggers()
}
