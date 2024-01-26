package processing

import (
	"errors"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processExternalObject(log logging.Logger, id database.ObjectId) pool.Status {

	_o, err := p.ob.GetObject(id)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			// TODO: object deleted
		}
		return pool.StatusFailed(err)
	}
	o := _o.(model.ExternalObject)

	elem, err := p.AssureElementObjectFor(log, o)
	if err != nil {
		return pool.StatusFailed(err)
	}

	log.Info("external object event -> trigger element {{element}}", "element", elem.Id())
	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}
