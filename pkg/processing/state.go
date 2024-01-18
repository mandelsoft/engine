package processing

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/version"
)

type State interface {
	GetLinks() []common.ElementId
	GetVersion() string

	GetVersionNode() version.Node
}
