package model

import (
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/logging"
)

func NewObjectIdForType(t string, s mmids.NameSource) mmids.ObjectId {
	return mmids.NewObjectId(t, s.GetNamespace(), s.GetName())
}

func NewElementIdForType(t string, s mmids.NameSource, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, s.GetNamespace(), s.GetName(), phase)
}

func PhaseId(id mmids.ElementId, phase mmids.Phase) mmids.ElementId {
	return NewElementIdForType(id.GetType(), id, phase)
}

func SlaveId(id mmids.ElementId, t string, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, id.GetNamespace(), id.GetName(), phase)
}

func NamedSlaveId(id mmids.ElementId, t string, name string, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, id.GetNamespace(), name, phase)
}

func ModifiedSlaveObjectVersion(log logging.Logger, e internal.Element, o ExternalObject) *string {
	if o == nil {
		return nil
	}
	old := e.GetObject().GetTargetState(e.GetPhase()).GetObjectVersion()
	mod := e.GetObject().GetExternalState(o, e.GetPhase()).GetVersion()
	if mod != old {
		log.Info("adjust target object version according to updated external object from {{old}} to {{new}}",
			"old", old, "new", mod)
		return &mod
	} else {
		log.Info("external object not modified")
		return nil
	}
}
