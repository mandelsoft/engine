package processing

import (
	"github.com/mandelsoft/engine/pkg/version"
)

type State interface {
	AddLink(ElementId)
	GetLinks() []ElementId
	GetVersion() string

	GetVersionNode() version.Node
}
