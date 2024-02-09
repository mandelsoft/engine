package processor

import (
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processNamespace(lctx model.Logging, log logging.Logger, name string) pool.Status {
	var err error
	ni := p.getNamespace(name)
	if ni != nil {
		ni.lock.Lock()
		defer ni.lock.Unlock()

		if ni.pendingOperation != nil {
			log := log.WithName(name).WithValues("namespace", name, "runid", ni.namespace.GetLock())
			err = ni.pendingOperation(lctx, log)
			if err == nil {
				ni.pendingOperation = nil
			}
		}
	}
	return pool.StatusCompleted(err)
}
