package processor

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

type namespaceReconciler struct {
	*reconciler
}

func newNamespaceReconciler(c *Controller) *namespaceReconciler {
	return &namespaceReconciler{&reconciler{controller: c}}
}

func (a *namespaceReconciler) Command(p pool.Pool, ctx pool.MessageContext, command pool.Command) pool.Status {
	// ctx = logging.ExcludeFromMessageContext[logging.Realm](ctx)
	ctx = ctx.WithContext(REALM)
	cmd, ns, _ := DecodeCommand(command)
	if cmd != CMD_NS {
		return pool.StatusFailed(fmt.Errorf("invalid processor command %q", command))
	}

	return newNamespaceReconcilation(a, ctx, ns).Reconcile()
}

type namespaceReconcilation struct {
	*namespaceReconciler
	logging.Logger
	*namespaceInfo
	lctx model.Logging
}

func newNamespaceReconcilation(r *namespaceReconciler, lctx model.Logging, name string) *namespaceReconcilation {
	return &namespaceReconcilation{
		namespaceReconciler: r,
		Logger:              lctx.Logger(REALM).WithName(name).WithValues("namespace", name),
		namespaceInfo:       r.getNamespaceInfo(name),
		lctx:                lctx,
	}
}
func (p *namespaceReconcilation) Reconcile() pool.Status {
	var err error
	if p.namespaceInfo != nil {
		p.lock.Lock()
		defer p.lock.Unlock()

		if p.pendingOperation != nil {
			err = p.pendingOperation(p.lctx, p.WithValues("runid", p.namespace.GetLock()))
			if err == nil {
				p.pendingOperation = nil
			}
		}
	}
	return pool.StatusCompleted(err)
}
