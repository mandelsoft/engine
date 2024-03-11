package watch

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/events"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/watch"
)

type Id struct {
	Kind      string `json:"type"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Phase     string `json:"phase"`
}

func (i Id) GetType() string {
	return i.Kind
}

func (i Id) GetNamespace() string {
	return i.Namespace
}

func (i Id) String() string {
	return fmt.Sprintf("%s/%s/%s:%s", i.Kind, i.Namespace, i.Name, i.Phase)
}

type Event struct {
	Node Id `json:"node"`

	Lock string `json:"lock"`

	Links []Id `json:"links,omitempty"`

	Status  string `json:"status"`
	Message string `json:"message"`
}

func (e Event) GetType() string {
	return e.Node.Kind
}

func (e Event) GetNamespace() string {
	return e.Node.Namespace
}

type Request struct {
	Kind      string `json:"type"`
	Namespace string `json:"namespace"`
}

func NewId(id ElementId) Id {
	return Id{
		Kind:      id.GetType(),
		Namespace: id.GetNamespace(),
		Name:      id.GetName(),
		Phase:     string(id.GetPhase()),
	}
}

func NewEvent(id Id, lock, status, message string, links ...Id) Event {
	return Event{
		Node:    id,
		Lock:    lock,
		Links:   links,
		Status:  status,
		Message: message,
	}
}

type Trigger interface {
	TriggerEvent(Event)
}

type Registry interface {
	watch.Registry[Request, Event]
	Trigger
}

type EventHandler = watch.EventHandler[Event]
type ObjectLister = events.ObjectLister[Event]

type registry struct {
	reg events.HandlerRegistry[Event]
}

var _ Registry = (*registry)(nil)

func NewRegistry(l events.ObjectLister[Event]) Registry {
	return &registry{
		reg: events.NewHandlerRegistry[Event](l),
	}
}

func (r *registry) RegisterWatchHandler(req Request, h EventHandler) {
	r.reg.RegisterHandler(h, true, req.Kind, req.Namespace)
}

func (r *registry) UnregisterWatchHandler(req Request, h EventHandler) {
	r.reg.UnregisterHandler(h, req.Kind, req.Namespace)
}

func (r *registry) TriggerEvent(event Event) {
	r.reg.TriggerEvent(event)
}
