package processor

import (
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/logging"
)

func (p *Processor) processExternalObject(log logging.Logger, id database.ObjectId) pool.Status {
	_o, err := p.processingModel.ObjectBase().GetObject(id)
	if err != nil {
		if errors.Is(err, database.ErrNotExist) {
			// TODO: object deleted
		}
		return pool.StatusFailed(err)
	}
	o := _o.(model.ExternalObject)
	oid := mmids.NewObjectIdFor(o)
	log = log.WithName(oid.String())
	elem, err := p.processingModel.AssureElementObjectFor(log, o)
	if err != nil {
		return pool.StatusFailed(err)
	}
	log = log.WithValues("extid", oid, "element", elem.Id())
	runid := elem.GetLock()
	if runid != "" && !p.processingModel.MetaModel().GetExternalType(id.GetType()).IsForeignControlled() {
		log.Info("external object event for {{extid}} with active run {{runid}} -> delay trigger", "runid", runid)
		return pool.StatusCompleted(fmt.Errorf("run %s active -> delay trigger", runid))
	}

	log.Info("external object event for {{extid}} -> trigger element {{element}}")
	p.Enqueue(CMD_EXT, elem)
	return pool.StatusCompleted(nil)
}
