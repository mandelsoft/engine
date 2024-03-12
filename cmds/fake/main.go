package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mandelsoft/engine/cmds/fake/objectspace"
	"github.com/mandelsoft/engine/cmds/fake/random"
	"github.com/mandelsoft/engine/cmds/fake/scenario1"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/spf13/pflag"
)

func Error(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+msg, args...)
	os.Exit(1)
}

func main() {
	var port int
	var pattern string
	var scenario string
	var consume bool

	flags := pflag.NewFlagSet("fake", pflag.ExitOnError)

	flags.IntVarP(&port, "port", "p", 8080, "server port")
	flags.StringVarP(&pattern, "pattern", "P", "/watch", "watch path pattern")
	flags.BoolVarP(&consume, "consumer", "c", false, "run consumer")
	flags.StringVarP(&scenario, "scenario", "s", "random", "scenario")

	err := flags.Parse(os.Args[1:])
	if err != nil {
		Error("invalid arguments: %s", err)
	}

	objectspace.Log.Info("starting")

	objects := objectspace.NewObjectSpace()
	objects.Set(&elemwatch.Event{
		Node: elemwatch.Id{
			Kind:      objectspace.MetaModel.NamespaceType(),
			Namespace: "",
			Name:      "",
			Phase:     "",
		},
		Status: "Ready",
	})
	objects.Set(&elemwatch.Event{
		Node: elemwatch.Id{
			Kind:      objectspace.MetaModel.NamespaceType(),
			Namespace: "",
			Name:      objectspace.NS,
			Phase:     "",
		},
		Status: "Ready",
	})
	server := elemwatch.NewServer(port, pattern, objects)

	ready, _, err := server.Start(context.Background())
	if err != nil {
		Error("cannot start server: %s", err.Error())
	}
	ready.Wait()
	if consume {
		Consume()
	}
	switch scenario {
	case "scenario1":
		scenario1.Scenario(objects)
		time.Sleep(time.Hour)
	default:
		random.Scenario(objects)

	}
}

func Consume() (watch.Syncher, error) {
	c := watch.NewClient[elemwatch.Request, elemwatch.Event]("ws://localhost:8080/watch")

	registration := elemwatch.Request{}
	return c.Register(context.Background(), registration, &handler{})
}

type handler struct {
}

func (h *handler) HandleEvent(e elemwatch.Event) {
	if e.GetType() == "" {
		objectspace.Log.Error("got event {{event}}", "event", e)
	} else {
		objectspace.Log.Trace("got event {{event}}", "event", e)
	}
}
