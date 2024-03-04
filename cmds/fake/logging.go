package main

import (
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
)

var REALM = logging.NewRealm("fake")

var log logging.Logger

func init() {
	logcfg := logrusl.Human(true)
	lctx := logging.DefaultContext()
	lctx.SetBaseLogger(logrusr.New(logcfg.NewLogrus()))

	lctx.AddRule(logging.NewConditionRule(logging.DebugLevel, logging.NewRealmPrefix("fake")))
	lctx.AddRule(logging.NewConditionRule(logging.DebugLevel, logging.NewRealmPrefix("engine")))
	lctx.AddRule(logging.NewConditionRule(logging.DebugLevel, logging.NewRealmPrefix("database")))
	log = lctx.Logger(REALM)
	log.Info("test {{value}}", "value", "value")
}
