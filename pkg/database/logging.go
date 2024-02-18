package database

import (
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("database", "Generic Data Store Support")

var log = logging.DynamicLogger(logging.DefaultContext(), REALM)
