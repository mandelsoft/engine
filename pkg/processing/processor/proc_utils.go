package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/logging"
)

type EffectiveVersion string
type FormalVersion string
type ObservedVersion string
type DetectedVersion string

func CalcEffectiveVersion(inputs model.Inputs, objvers string) EffectiveVersion {
	keys := maputils.Keys(inputs, CompareElementId)

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

func TriggeringObject(p *Controller, id TypeId) []string {
	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(id)
	if trigger == nil {
		return nil
	}
	return []string{*trigger}
}

func UpdateObjects(p *Controller, id TypeId) []string {
	return p.processingModel.MetaModel().GetExternalTypesFor(id)
}

func (p *Controller) triggerLinks(log logging.Logger, msg string, links ...ElementId) {
	log.Info(fmt.Sprintf("trigger %s elements", msg))
	for _, l := range links {
		log.Info(fmt.Sprintf(" - trigger %s element {{parent}}", msg), "parent", l)
		p.EnqueueKey(CMD_ELEM, l)
	}
}

func (p *Controller) updateRunId(r Reconcilation, verb string, elem Element, rid RunId) error {
	types := p.MetaModel().GetExternalTypesFor(elem.Id().TypeId())
	for _, t := range types {
		extid := database.NewObjectId(t, elem.GetNamespace(), elem.GetName())
		o, err := p.Objectbase().GetObject(extid)
		if err != nil {
			if errors.Is(err, database.ErrNotExist) {
				continue
			}
			r.Error("cannot get external object {{extid}}", "extid", extid, "error", err)
			return err
		}
		err = o.(model.ExternalObject).UpdateStatus(r.LoggingContext(), p.Objectbase(), elem.Id(), model.StatusUpdate{
			RunId:           &rid,
			DetectedVersion: generics.Pointer(""),
		})
		if err != nil {
			r.Error(fmt.Sprintf("cannot %s run for external object  {{extid}}", verb), "extid", extid, "error", err)
			return err
		}
	}
	return nil
}

func (p *Controller) getTriggeringExternalObject(id ElementId) (bool, model.ExternalObject, error) {
	found := false

	trigger := p.processingModel.MetaModel().GetTriggerTypeForElementType(id.TypeId())
	if trigger != nil {
		found = true
		oid := model.NewObjectIdForType(*trigger, id)
		o, err := p.processingModel.ObjectBase().GetObject(oid)
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) {
				return false, nil, err
			}
		}
		if o != nil {
			return true, o.(model.ExternalObject), nil
		}
	}
	return found, nil, nil
}

func (p *Controller) isDeleting(objs ...model.ExternalObject) bool {
	for _, o := range objs {
		if o != nil && o.IsDeleting() {
			return true
		}
	}
	return false
}

func (p *Controller) verifyLinks(e _Element, links ...ElementId) error {
	for _, l := range links {
		if err := p.processingModel.MetaModel().VerifyLink(e.Id(), l); err != nil {
			return err
		}
	}
	return nil
}

func formalInputVersions(inputs model.Inputs) []string {
	return maputils.Values(maputils.Transform(inputs, mapInputsToVersions), version.CompareId)
}

func mapInputsToVersions(id ElementId, state model.OutputState) (version.Id, string) {
	return version.NewIdFor(id), state.GetFormalVersion()
}
