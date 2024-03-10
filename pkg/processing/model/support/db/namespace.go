package db

import (
	"github.com/mandelsoft/engine/pkg/processing/mmids"
)

type Namespace struct {
	ObjectMeta `json:",inline"`

	RunLock mmids.RunId `json:"runLock"`
}

var _ DBNamespace = (*Namespace)(nil)

func (n *Namespace) GetRunLock() mmids.RunId {
	return n.RunLock
}

func (n *Namespace) SetRunLock(id mmids.RunId) {
	n.RunLock = id
}

func (n *Namespace) GetStatusValue() string {
	if n.RunLock != "" {
		return "Locked"
	}
	return "Unlocked"
}
