package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/version"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type EffectiveVersion string
type ObservedVersion string

func CalcEffectiveVersion(inputs model.Inputs, objvers string) EffectiveVersion {
	keys := utils.MapKeys(inputs)
	slices.SortFunc(keys, utils.CompareStringable[ElementId]) // Goland

	hash := sha256.New()
	hash.Write([]byte(objvers))
	for _, id := range keys {
		hash.Write([]byte(id.String()))
		hash.Write([]byte(inputs[id].GetOutputVersion()))
	}

	h := hash.Sum(nil)
	return EffectiveVersion(hex.EncodeToString(h[:]))
}

func GetResultState(args ...interface{}) model.OutputState {
	for _, a := range args {
		switch opt := a.(type) {
		case model.OutputState:
			return opt
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (p *Processor) forExtObjects(log logging.Logger, e _Element, f func(log logging.Logger, object model.ExternalObject) error) error {
	exttypes := p.processingModel.MetaModel().GetAssignedExternalTypes(e.Id().TypeId())
	for _, t := range exttypes {
		id := NewObjectId(t, e.GetNamespace(), e.GetName())
		log = log.WithValues("extid", id)
		_o, err := p.processingModel.ObjectBase().GetObject(database.NewObjectId(id.GetType(), id.GetNamespace(), id.GetName()))
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				log.Error("cannot get external object {{extid}}", "error", err)
				return err
			}
			log.Info("external object {{extid}} not found -> skip")
			continue
		}
		err = f(log, _o.(model.ExternalObject))
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) updateStatus(lctx model.Logging, log logging.Logger, elem _Element, status model.Status, message string, args ...any) error {
	update := model.StatusUpdate{
		Status:  &status,
		Message: &message,
	}
	keys := []interface{}{
		"newstatus", status,
		"message", message,
	}

	for _, a := range args {
		switch opt := a.(type) {
		case RunId:
			update.RunId = utils.Pointer(opt)
			keys = append(keys, "runid", update.RunId)
		case model.OutputState:
			update.ResultState = opt
			keys = append(keys, "result", DescribeObject(opt))
		case ObservedVersion:
			update.ObservedVersion = utils.Pointer(string(opt))
			keys = append(keys, "observed version", opt)
		case EffectiveVersion:
			update.EffectiveVersion = utils.Pointer(string(opt))
			keys = append(keys, "effective version", opt)
		default:
			panic(fmt.Sprintf("unknown status argument type %T", a))
		}
	}
	log.Info(" updating status of external objects to {{newstatus}}: {{message}}", keys...)

	mod := func(log logging.Logger, o model.ExternalObject) error {
		return o.UpdateStatus(lctx, p.processingModel.ObjectBase(), elem.Id(), update)
	}
	return p.forExtObjects(log, elem, mod)
}

func (p *Processor) triggerLinks(log logging.Logger, msg string, links ...ElementId) {
	log.Info(fmt.Sprintf("trigger %s elements", msg))
	for _, l := range links {
		log.Info(fmt.Sprintf(" - trigger %s element {{parent}}", msg), "parent", l)
		p.EnqueueKey(CMD_ELEM, l)
	}
}

func (p *Processor) triggerChildren(log logging.Logger, ni *namespaceInfo, elem _Element, release bool) {
	ni.lock.Lock()
	defer ni.lock.Unlock()
	// TODO: dependency check must be synchronized with this trigger

	id := elem.Id()
	log.Info("triggering children for {{element}} (checking {{amount}} elements in namespace)", "amount", len(ni.elements))
	for _, e := range ni.elements {
		if e.GetProcessingState() != nil {
			links := e.GetProcessingState().GetLinks()
			log.Debug("- elem {{child}} has target links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					log.Info("  trigger pending element {{waiting}} active in {{target-runid}}", "waiting", e.Id(), "target-runid", e.GetLock())
					p.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		} else if e.GetStatus() != model.STATUS_DELETED && e.GetCurrentState() != nil {
			links := e.GetCurrentState().GetLinks()
			log.Debug("- elem {{child}} has current links {{links}}", "child", e.Id(), "links", links)
			for _, l := range links {
				if l == id {
					log.Info("  trigger pending element {{waiting}}", "waiting", e.Id(), "target-runid", e.GetLock())
					p.EnqueueKey(CMD_ELEM, e.Id())
				}
			}
		}
	}
	if release {
		elem.SetProcessingState(nil)
	}
}

func (p *Processor) updateRunId(lctx model.Logging, log logging.Logger, verb string, elem Element, rid RunId) error {
	types := p.processingModel.MetaModel().GetTriggeringTypesForElementType(elem.Id().TypeId())
	for _, t := range types {
		extid := database.NewObjectId(t, elem.GetNamespace(), elem.GetName())
		o, err := p.processingModel.ObjectBase().GetObject(extid)
		if err != nil {
			if errors.Is(err, database.ErrNotExist) {
				continue
			}
			log.Error("cannot get external object {{extid}}", "extid", extid, "error", err)
			return err
		}
		err = o.(model.ExternalObject).UpdateStatus(lctx, p.processingModel.ObjectBase(), elem.Id(), model.StatusUpdate{
			RunId:           &rid,
			DetectedVersion: utils.Pointer(""),
		})
		if err != nil {
			log.Error(fmt.Sprintf("cannot %s run for external object  {{extid}}", verb), "extid", extid, "error", err)
			return err
		}
	}
	return nil
}

func (p *Processor) getTriggeringExternalObjects(id ElementId) (bool, []model.ExternalObject, error) {
	var result []model.ExternalObject
	found := false

	for _, ext := range p.processingModel.MetaModel().GetTriggeringTypesForElementType(id.TypeId()) {
		found = true
		oid := model.NewObjectIdForType(ext, id)
		o, err := p.processingModel.ObjectBase().GetObject(oid)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				return false, nil, err
			}
		}
		if o != nil {
			result = append(result, o.(model.ExternalObject))
		}
	}
	return found, result, nil
}

func (p *Processor) isDeleting(objs ...model.ExternalObject) bool {
	for _, o := range objs {
		if o.IsDeleting() {
			return true
		}
	}
	return false
}

func formalInputVersions(inputs model.Inputs) []string {
	return utils.MapElements(utils.TransformMap(inputs, mapInputsToVersions), version.CompareId)
}

func mapInputsToVersions(id ElementId, state model.OutputState) (version.Id, string) {
	return version.NewIdFor(id), state.GetFormalVersion()
}
