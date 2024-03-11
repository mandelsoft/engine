package watch

import (
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("engine/watch", "engine watch endpoint")

var log = logging.DefaultContext().Logger(REALM)
