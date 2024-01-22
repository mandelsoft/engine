package processing

import (
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processNamespace(lctx logging.Context, name string) pool.Status {
	ns := p.GetNamespace(name)
	if ns != nil {
		log := lctx.Logger(logging.NewAttribute("namespace", name), logging.NewAttribute("runid", ns.namespace.GetLock()))
		ns.lock.Lock()
		defer ns.lock.Unlock()
		err := ns.ClearLocks(log, p)
		return pool.StatusCompleted(err)
	}
	return pool.StatusCompleted()
}
