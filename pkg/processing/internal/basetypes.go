package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/logging"
)

type Encoding = database.Encoding[Object]
type SchemeTypes = database.SchemeTypes[Object]
type Scheme = database.Scheme[Object]

type Logging = logging.AttributionContext
