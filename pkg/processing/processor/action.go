package processor

import (
	"fmt"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/generics"

	"github.com/mandelsoft/engine/pkg/pool"
)

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
	return cmd, "", generics.Pointer(NewElementId(t, ns, n, Phase(p)))
}
