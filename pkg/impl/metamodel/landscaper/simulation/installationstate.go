package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodel/landscaper"
)

type InstallationState struct {
	InternalObject[landscaper.Installation] `json:",inline"`
}

var _ landscaper.InstallationState = (*InstallationState)(nil)

func NewInstallationState(name string) *InstallationState {
	return newVersionedObject[InstallationState](landscaper.TYPE_INSTALLATION_STATE, name)
}
