package main

import (
	"context"
	"fmt"
	"os"
	"time"

	dbservice "github.com/mandelsoft/engine/pkg/database/service"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/controllers"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/processor"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/server"
	"github.com/mandelsoft/engine/pkg/service"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/mandelsoft/logging"
	"github.com/spf13/pflag"
)

func Error(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+msg, args...)
	os.Exit(1)
}

func main() {
	var port int
	var watchPattern string
	var consume bool
	var database string = "."
	var files string
	var level string = "info"
	var delay time.Duration

	flags := pflag.NewFlagSet("engine", pflag.ExitOnError)

	flags.IntVarP(&port, "port", "p", 8080, "server port")
	flags.StringVarP(&watchPattern, "pattern", "P", "/watch", "watch path pattern")
	flags.BoolVarP(&consume, "consumer", "c", false, "run consumer")
	flags.StringVarP(&level, "log-level", "L", level, "log level")
	flags.StringVarP(&database, "database", "d", database, "database path")
	flags.StringVarP(&files, "files", "F", database, "file server base directory for /ui")
	flags.DurationVarP(&delay, "delay", "D", 0, "processing delay (duration)")

	err := flags.Parse(os.Args[1:])
	if err != nil {
		Error("invalid arguments: %s", err)
	}

	l, err := logging.ParseLevel(level)
	if err != nil {
		Error("invalid log level %q", level)
	}
	lctx := logging.DefaultContext()
	lctx.AddRule(logging.NewConditionRule(l, logging.NewRealmPrefix("engine")))
	lctx.AddRule(logging.NewConditionRule(l, logging.NewRealmPrefix("database")))

	dbspec := filesystem.NewSpecification[db.Object](database)
	mspec := sub.NewModelSpecification("expression", dbspec)
	m, err := model.NewModel(mspec)
	if err != nil {
		Error("cannot create model: %s", err.Error())
	}

	proc, err := processor.NewProcessor(lctx, m, 1, version.Composed)
	if err != nil {
		Error("cannot create processor: %s", err.Error())
	}

	if delay > 0 {
		proc.SetDelay(delay)
	}
	odb := objectbase.GetDatabase[db.Object](proc.Model().ObjectBase())
	cntr := controllers.NewExpressionController(lctx, 1, odb)

	srv := server.NewServer(port, true, 20*time.Second)
	log.Info("serving watch on {{path}}", "path", watchPattern)
	proc.RegisterWatchHandler(srv, watchPattern)
	dbservice.New(odb, "/db").RegisterHandler(srv)

	if files != "" {
		dir, err := server.NewDirectoryHandlerFor(files, "/ui")
		if err != nil {
			Error("cannot create file server: %s", err.Error())
		}
		log.Info("serving ui on /ui from {{path}}", "path", files)
		dir.RegisterHandler(srv)
	}

	reg := service.New(context.Background())
	reg.Add(cntr)
	reg.Add(proc)
	reg.Add(srv)

	err = reg.Start()
	if err != nil {
		Error("cannot start services: %s", err.Error())
	}
	if consume {
		log.Info("registering watch")
		Consume(watchPattern)
	}
	reg.Wait()
}

func Consume(watchPattern string) (watch.Syncher, error) {
	c := watch.NewClient[elemwatch.Request, elemwatch.Event]("ws://localhost:8080" + watchPattern)

	registration := elemwatch.Request{}
	return c.Register(context.Background(), registration, &handler{})
}

type handler struct {
}

func (h *handler) HandleEvent(e elemwatch.Event) {
	log.Info("got event {{event}}", "event", e)
}
