package main

import (
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/logging/logrusr"
)

var REALM = logging.DefineRealm("engine", "processing engine")

var log logging.Logger

func init() {
	logcfg := logrusl.Human(true)
	lctx := logging.DefaultContext()
	lctx.SetBaseLogger(logrusr.New(logcfg.NewLogrus()))
	log = lctx.Logger(REALM)
}
