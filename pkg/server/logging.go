package server

import (
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("server", "http server")

var log logging.Logger = logging.DefaultContext().Logger(REALM)
