package database

import (
	"github.com/mandelsoft/engine/pkg/events"
)

type ObjectLister = events.ObjectLister[ObjectId]
type EventHandler = events.EventHandler[ObjectId]
type HandlerRegistration = events.HandlerRegistration[ObjectId]
type HandlerRegistrationTest = events.HandlerRegistrationTest[ObjectId]
type HandlerRegistry = events.HandlerRegistry[ObjectId]

func NewHandlerRegistry(l ObjectLister) HandlerRegistry {
	return events.NewHandlerRegistry[ObjectId](NewObjectIdFor, l)
}
