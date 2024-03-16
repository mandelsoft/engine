package processor

import (
	"github.com/mandelsoft/engine/pkg/processing/model"
	"k8s.io/apimachinery/pkg/util/sets"
)

var extTriggerable = sets.Set[model.Status]{}
var processable = sets.Set[model.Status]{}

func init() {
	extTriggerable.Insert(
		model.STATUS_INITIAL,
		model.STATUS_PENDING,
		model.STATUS_INVALID,
		model.STATUS_PREPARING,
		model.STATUS_BLOCKED,
		model.STATUS_COMPLETED,
		model.STATUS_FAILED,
	)

	processable.Insert(
		model.STATUS_PROCESSING,
		model.STATUS_DELETING,
		model.STATUS_WAITING,
	)
}

func isExtTriggerable(e _Element) bool {
	return extTriggerable.Has(e.GetStatus())
}

func isProcessable(e _Element) bool {
	return processable.Has(e.GetStatus())
}

func isFinal(e _Element) bool {
	return model.IsFinalStatus(e.GetStatus())
}
