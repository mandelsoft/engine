package main

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/spf13/cobra"
)

type Get struct {
	cmd *cobra.Command

	mainopts *Options
	format   string
}

func NewGet(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <type> {<object>} <options>",
		Short: "get objects from database",
	}

	c := &Get{
		cmd: cmd,
	}
	c.cmd.RunE = func(cmd *cobra.Command, args []string) error { return c.Run(args) }
	flags := cmd.Flags()
	flags.StringVarP(&c.format, "output", "o", "", "output format")
	return cmd
}

func (c *Get) Run(args []string) error {

	if len(args) < 1 {
		return fmt.Errorf("object type required")
	}
	typ := args[1]
	if typ == "" {
		return fmt.Errorf("non-empty type required")
	}
	var list []database.Object

	for _, arg := range args {
		ns := c.mainopts.namespace
		for strings.HasPrefix(arg, "/") {
			ns = ""
			arg = arg[1:]
		}
		i := strings.LastIndex(arg, "/")
		if i > 0 {
			if ns != "" {
				ns = ns + "/" + arg[:i]
			} else {
				ns = arg[:i]
			}
			arg = arg[i+1:]
		}

		get := Must(http.Get(URL + path.Join(TYPE_A, NS, "o1")))
	}
	return nil
}
