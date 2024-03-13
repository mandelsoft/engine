package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"

	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/watch"
	"github.com/spf13/cobra"
)

type Watch struct {
	cmd *cobra.Command

	mainopts *Options
	closure  bool
}

func NewWatch(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch <namespace> <options>",
		Short: "watch engine processing",
	}
	TweakCommand(cmd)

	c := &Watch{
		cmd:      cmd,
		mainopts: opts,
	}
	c.cmd.RunE = func(cmd *cobra.Command, args []string) error { return c.Run(args) }
	flags := cmd.Flags()
	flags.BoolVarP(&c.closure, "closure", "c", false, "namespace closure")
	return cmd
}

func (c *Watch) Run(args []string) error {

	u, err := url.Parse(c.mainopts.GetURL())
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}

	ns := c.mainopts.namespace
	if len(args) > 1 {
		return fmt.Errorf("only one optional namespace argument possible")
	}
	if len(args) == 1 {
		ns = args[0]
	}

	a := fmt.Sprintf("%s://%s/watch", scheme, u.Host)

	s, err := Consume(c.cmd.OutOrStdout(), a, c.closure, ns)
	if err != nil {
		return err
	}
	s.Wait()
	return nil
}

func Consume(w io.Writer, address string, closure bool, ns string) (watch.Syncher, error) {
	c := watch.NewClient[elemwatch.Request, elemwatch.Event](address)

	registration := elemwatch.Request{Flat: !closure, Namespace: ns}
	return c.Register(context.Background(), registration, &handler{w})
}

type handler struct {
	w io.Writer
}

func (h *handler) HandleEvent(e elemwatch.Event) {
	data, _ := json.Marshal(e)
	fmt.Fprintf(h.w, "%s\n", string(data))
}
