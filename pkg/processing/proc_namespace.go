package processing

import (
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processNamespace(lctx logging.Context, name string) pool.Status {
	var err error
	ni := p.GetNamespace(name)
	if ni != nil {
		ni.lock.Lock()
		defer ni.lock.Unlock()

		if ni.pendingOperation != nil {
			log := lctx.Logger(logging.NewAttribute("namespace", name), logging.NewAttribute("runid", ni.namespace.GetLock()))
			err = ni.pendingOperation(log)
			if err == nil {
				ni.pendingOperation = nil
			}
		}
	}
	return pool.StatusCompleted(err)
}
