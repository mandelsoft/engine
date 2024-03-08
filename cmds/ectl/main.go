package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func Error(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+msg, args...)
	os.Exit(1)
}

type Options struct {
	address   string
	namespace string
}

func (o *Options) GetURL() string {
	a := o.address
	if !strings.HasPrefix(a, "http://") && !strings.HasPrefix(a, "https://") {
		a = "https://" + a
	}
}

func main() {
	opts := &Options{}

	opts.address = os.Getenv("ENGINE_SERVER")
	if opts.address == "" {
		opts.address = "http://localhost:8080"
	}
	opts.namespace = os.Getenv("ENGINE_NAMESPACE")

	maincmd := &cobra.Command{
		Use:   "ectl <options> <cmd> <args>",
		Short: "execute engine command",
		Long: `
This command can be used to manipulate the object base used by
the processing engine.
`,
		Run: nil,
	}

	flags := maincmd.Flags()

	flags.StringVarP(&opts.namespace, "namespace", "n", opts.namespace, "namespace for operation")
	flags.StringVarP(&opts.address, "server", "s", opts.address, "engine server")

}
