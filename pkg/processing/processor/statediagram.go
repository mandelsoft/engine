package processor

import (
	"github.com/mandelsoft/engine/pkg/processing/model"
	"k8s.io/apimachinery/pkg/util/sets"
)

var extTriggerable = sets.Set[model.Status]{}

func init() {
	extTriggerable.Insert(
		model.STATUS_INITIAL,
		model.STATUS_PREPARING,
		model.STATUS_BLOCKED,
		model.STATUS_COMPLETED,
		model.STATUS_FAILED,
	)
}

func isExtTriggerable(e _Element) bool {
	return extTriggerable.Has(e.GetStatus())
}
