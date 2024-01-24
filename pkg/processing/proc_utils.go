package processing

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type EffectiveVersion string

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

func (p *Processor) updateStatus(elem Element, status string, message string, args ...any) error {
	for _, t := range p.mm.GetTriggeringTypesForInternalType(elem.GetType()) {
		oid := database.NewObjectId(t, elem.GetNamespace(), elem.GetName())

		_o, err := p.ob.GetObject(oid)
		if err != nil {
			return err
		}
		o := _o.(model.ExternalObject)

		status := common.StatusUpdate{
			RunId:           nil,
			DetectedVersion: nil,
			ObservedVersion: nil,
			Status:          &status,
			Message:         &message,
			InternalState:   nil,
		}
		for _, a := range args {
			switch opt := a.(type) {
			case model.RunId:
				status.RunId = utils.Pointer(opt)
			case model.InternalState:
				status.InternalState = opt
			case EffectiveVersion:
				status.EffectiveVersion = utils.Pointer(string(opt))
			default:
				panic(fmt.Sprintf("unknown status argument type %T", a))
			}
		}
		err = o.UpdateStatus(p.ob, elem.Id(), status)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) triggerChildren(ni *NamespaceInfo, elem Element, release bool) {
	ni.lock.Lock()
	defer ni.lock.Unlock()
	// TODO: dependency check must be synchronized with this trigger

	for _, e := range ni.elements {
		if e.GetTargetState() != nil {
			for _, l := range e.GetTargetState().GetLinks() {
				p.EnqueueKey("", l)
			}
		}
		if e.GetCurrentState() != nil {
			for _, l := range e.GetCurrentState().GetLinks() {
				p.EnqueueKey("", l)
			}
		}
	}
	if release {
		elem.SetTargetState(nil)
	}
}

func (p *Processor) updateRunId(log logging.Logger, verb string, elem Element, rid model.RunId) error {
	types := p.mm.GetTriggeringTypesForElementType(elem.Id().TypeId())
	for _, t := range types {
		extid := database.NewObjectId(t, elem.GetNamespace(), elem.GetName())
		o, err := p.ob.GetObject(extid)
		if err != nil {
			if errors.Is(err, database.ErrNotExist) {
				continue
			}
			log.Error("cannot get external object {{extid}}", "extid", extid, "error", err)
			return err
		}
		err = o.(model.ExternalObject).UpdateStatus(p.ob, elem.Id(), common.StatusUpdate{
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
