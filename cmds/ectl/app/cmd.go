package app

import (
	"os"
	"strings"

	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
	"github.com/spf13/cobra"
)

type Options struct {
	address   string
	namespace string
	fs        vfs.FileSystem
}

func (o *Options) GetURL() string {
	a := o.address
	if !strings.HasPrefix(a, "http://") && !strings.HasPrefix(a, "https://") {
		a = "https://" + a
	}
	if !strings.HasSuffix(a, "/") {
		a += "/"
	}
	return a + "db/"
}

func New(fss ...vfs.FileSystem) *cobra.Command {
	opts := &Options{
		fs: utils.OptionalDefaulted(vfs.FileSystem(osfs.OsFs), fss...),
	}

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
		Run:              nil,
		TraverseChildren: true,
	}

	flags := maincmd.Flags()

	flags.StringVarP(&opts.namespace, "namespace", "n", opts.namespace, "namespace for operation")
	flags.StringVarP(&opts.address, "server", "s", opts.address, "engine server")

	maincmd.AddCommand(NewGet(opts))
	maincmd.AddCommand(NewApply(opts))
	return maincmd
}
