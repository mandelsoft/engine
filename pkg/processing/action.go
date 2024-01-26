package processing

import (
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/pool"
	"github.com/mandelsoft/engine/pkg/utils"
)

const ACTION_CMD = "element"

type action struct {
	proc *Processor
}

var _ pool.Action = (*action)(nil)

func (a *action) Reconcile(p pool.Pool, ctx pool.MessageContext, id database.ObjectId) pool.Status {
	return a.proc.processExternalObject(a.proc.logging.Logger(ctx...), id)
}

func (a *action) Command(p pool.Pool, ctx pool.MessageContext, command pool.Command) pool.Status {
	cmd, ns, id := DecodeCommand(command)
	if cmd == CMD_NS {
		a.proc.processNamespace(a.proc.logging.Logger(ctx...), ns)
	}
	if id != nil {
		return a.proc.processElement(a.proc.logging.Logger(ctx...), cmd, *id)
	} else {
		return pool.StatusFailed(fmt.Errorf("invalid processor command %q", command))
	}
}

func EncodeElement(cmd string, id ElementId) pool.Command {
	return pool.Command(fmt.Sprintf("%s:%s", cmd, id.String()))
}

func EncodeNamespace(ns string) pool.Command {
	return pool.Command(fmt.Sprintf("%s:%s", CMD_NS, ns))
}

func DecodeCommand(c pool.Command) (string, string, *ElementId) {
	s := string(c)
	i := strings.Index(s, ":")
	if i < 0 {
		return "", "", nil
	}
	cmd := s[:i]
	s = s[i+1:]

	if cmd == CMD_NS {
		return cmd, s, nil
	}
	i = strings.Index(s, "/")
	if i < 0 {
		return "", "", nil
	}
	t := s[:i]
	s = s[i+1:]

	i = strings.LastIndex(s, ":")
	if i < 0 {
		return "", "", nil
	}
	ns := s[:i]
	p := s[i+1:]

	var n string
	i = strings.LastIndex(ns, "/")
	if i < 0 {
		n = ns
		ns = ""
	} else {
		n = ns[i+1:]
		ns = ns[:i]
	}
	return cmd, "", utils.Pointer(common.NewElementId(t, ns, n, model.Phase(p)))
}
