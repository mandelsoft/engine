package database

import (
	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("database", "Generic Data Store Support")

var Log = logging.DynamicLogger(logging.DefaultContext(), REALM)
