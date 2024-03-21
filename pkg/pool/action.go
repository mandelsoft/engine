package pool

import (
	"fmt"
	"time"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Action interface {
	Reconcile(Pool, MessageContext, database.ObjectId) Status
	Command(Pool, MessageContext, Command) Status
}

type DefaultAction struct{}

func (a DefaultAction) Reconcile(_ Pool, _ MessageContext, id database.ObjectId) Status {
	return StatusFailed(fmt.Errorf("unexpected reconcile request for %q", id))
}

func (a DefaultAction) Command(_ Pool, _ MessageContext, c Command) Status {
	return StatusFailed(fmt.Errorf("unexpected command request for %q", c))
}

type Command string

func (c Command) String() string {
	return string(c)
}

type ObjectType string

func (o ObjectType) String() string {
	return string(o)
}

const tick = 30 * time.Second
const tickCmd = "TICK"

type ActionTargetSpec interface {
	String() string
}

type actions []Action

func (l actions) add(a Action) actions {
	for _, r := range l {
		if r == a {
			return l
		}
	}
	return append(l, a)
}

type actionMapping struct {
	values   map[interface{}]actions
	matchers map[utils.Matcher]actions
}

func newActionMapping() *actionMapping {
	return &actionMapping{
		values:   map[interface{}]actions{},
		matchers: map[utils.Matcher]actions{},
	}
}

func (am *actionMapping) getAction(key interface{}) actions {
	i := am.values[key]
	if i == nil {
		cmd, ok := key.(Command)
		if ok {
			for m, i := range am.matchers {
				if m.Match(string(cmd)) {
					return i
				}
			}
		}
		return nil
	}
	return i
}

func (am *actionMapping) addAction(key ActionTargetSpec, a Action) {
	switch k := key.(type) {
	case utils.StringMatcher:
		am.values[string(k)] = am.values[string(k)].add(a)
	case utils.Matcher:
		am.matchers[k] = am.matchers[k].add(a)
	default:
		am.values[k] = am.values[k].add(a)
	}
}
