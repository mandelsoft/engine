package support

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"

	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

func DefaultInputVersion(inputs model.Inputs) string {
	keys := utils.MapKeys(inputs)
	slices.SortFunc(keys, utils.CompareStringable[mmids.ElementId])

	hash := sha256.New()
	for _, k := range keys {
		v := inputs[k].GetOutputVersion()
		hash.Write([]byte(v))
	}
	h := hash.Sum(nil)
	return hex.EncodeToString(h[:])
}

func UpdateField[T any](field *T, value *T, mod ...*bool) bool {
	if value != nil && !reflect.DeepEqual(field, value) {
		*field = *value
		if len(mod) > 0 {
			*mod[0] = true
		}
		return true
	}
	return false
}

func UpdatePointerField[T any](field **T, value *T, mod ...*bool) bool {
	if value != nil && (*field == nil || !reflect.DeepEqual(*field, value)) {
		*field = value
		if len(mod) > 0 {
			*mod[0] = true
		}
		return true
	}
	return false
}

func AssureElement[I InternalDBObject, R any](log logging.Logger, ob objectbase.Objectbase, typ mmids.TypeId, name string, req model.Request, mod func(i I) (R, bool)) (R, InternalObject, bool, error) {
	var _nil R

	if !req.Model.MetaModel().HasElementType(typ) {
		return _nil, nil, false, fmt.Errorf("unknown element type %q", typ)
	}

	eid := mmids.NewElementIdForType(typ, req.Element.GetNamespace(), name)
	t := req.ElementAccess.GetElement(eid)
	if t == nil {
		tolock := req.Model.MetaModel().GetDependentTypePhases(typ)
		i, err := ob.CreateObject(eid)
		if err != nil {
			return _nil, nil, false, err
		}
		r, err := wrapped.Modify(ob, i.(wrapper.Object[DBObject]), func(_o DBObject) (R, bool) {
			o := _o.(I)
			for _, ph := range tolock {
				i.(InternalObject).GetPhaseInfoFor(o, ph).TryLock(req.Element.GetLock())
			}
			r, _ := mod(o)
			return r, true
		})
		if err == nil {
			log.Info("created required internal object {{newelem}}", "newelem", eid.ObjectId())
		}
		return r, i.(InternalObject), true, err
	}
	log.Info("required element {{newelem}} already exists", "newelem", eid)
	return _nil, t.GetObject().(InternalObject), false, nil
}

type StatusSource interface {
	GetStatus() model.Status
}

var statusmerge = map[model.Status]map[model.Status]model.Status{
	model.STATUS_INITIAL: {
		model.STATUS_INITIAL:    model.STATUS_INITIAL,
		model.STATUS_COMPLETED:  model.STATUS_COMPLETED,
		model.STATUS_BLOCKED:    model.STATUS_BLOCKED,
		model.STATUS_FAILED:     model.STATUS_FAILED,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_COMPLETED: {
		model.STATUS_INITIAL:    model.STATUS_COMPLETED,
		model.STATUS_COMPLETED:  model.STATUS_COMPLETED,
		model.STATUS_BLOCKED:    model.STATUS_BLOCKED,
		model.STATUS_FAILED:     model.STATUS_FAILED,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_COMPLETED,
	},
	model.STATUS_BLOCKED: {
		model.STATUS_INITIAL:    model.STATUS_BLOCKED,
		model.STATUS_COMPLETED:  model.STATUS_BLOCKED,
		model.STATUS_BLOCKED:    model.STATUS_BLOCKED,
		model.STATUS_FAILED:     model.STATUS_BLOCKED,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_FAILED: {
		model.STATUS_INITIAL:    model.STATUS_FAILED,
		model.STATUS_COMPLETED:  model.STATUS_FAILED,
		model.STATUS_BLOCKED:    model.STATUS_BLOCKED,
		model.STATUS_FAILED:     model.STATUS_FAILED,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_PENDING: {
		model.STATUS_INITIAL:    model.STATUS_PENDING,
		model.STATUS_COMPLETED:  model.STATUS_PENDING,
		model.STATUS_BLOCKED:    model.STATUS_PENDING,
		model.STATUS_FAILED:     model.STATUS_PENDING,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_PREPARING: {
		model.STATUS_INITIAL:    model.STATUS_PREPARING,
		model.STATUS_COMPLETED:  model.STATUS_PREPARING,
		model.STATUS_BLOCKED:    model.STATUS_PREPARING,
		model.STATUS_FAILED:     model.STATUS_PREPARING,
		model.STATUS_PENDING:    model.STATUS_PREPARING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_WAITING: {
		model.STATUS_INITIAL:    model.STATUS_WAITING,
		model.STATUS_COMPLETED:  model.STATUS_WAITING,
		model.STATUS_BLOCKED:    model.STATUS_WAITING,
		model.STATUS_FAILED:     model.STATUS_WAITING,
		model.STATUS_PENDING:    model.STATUS_WAITING,
		model.STATUS_PREPARING:  model.STATUS_WAITING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
	model.STATUS_PROCESSING: {
		model.STATUS_INITIAL:    model.STATUS_PROCESSING,
		model.STATUS_COMPLETED:  model.STATUS_PROCESSING,
		model.STATUS_BLOCKED:    model.STATUS_PROCESSING,
		model.STATUS_FAILED:     model.STATUS_PROCESSING,
		model.STATUS_PENDING:    model.STATUS_PROCESSING,
		model.STATUS_PREPARING:  model.STATUS_PROCESSING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_PROCESSING,
		model.STATUS_DELETED:    model.STATUS_PROCESSING,
	},
	model.STATUS_DELETED: {
		model.STATUS_INITIAL:    model.STATUS_DELETED,
		model.STATUS_COMPLETED:  model.STATUS_COMPLETED,
		model.STATUS_BLOCKED:    model.STATUS_BLOCKED,
		model.STATUS_FAILED:     model.STATUS_FAILED,
		model.STATUS_PENDING:    model.STATUS_PENDING,
		model.STATUS_PREPARING:  model.STATUS_PREPARING,
		model.STATUS_PROCESSING: model.STATUS_PROCESSING,
		model.STATUS_WAITING:    model.STATUS_WAITING,
		model.STATUS_DELETED:    model.STATUS_DELETED,
	},
}

func mergeStatus(a, b model.Status) model.Status {
	n := statusmerge[a]
	if n != nil {
		m, ok := n[b]
		if ok {
			return m
		}
		return a
	} else {
		return b
	}
}

func CombinedPhaseStatus[I InternalDBObject](access PhaseStateAccess[I], o I) model.Status {
	status := model.STATUS_INITIAL
	for _, a := range access {
		status = mergeStatus(status, a(o).GetStatus())
	}
	return status
}

func CombinedStatus(ss ...StatusSource) model.Status {
	status := model.STATUS_INITIAL
	for _, s := range ss {
		status = mergeStatus(status, s.GetStatus())
	}
	return status
}
