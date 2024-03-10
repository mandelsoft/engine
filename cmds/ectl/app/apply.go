package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/glob"
	"github.com/mandelsoft/engine/pkg/impl/database/filesystem"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/vfs/pkg/vfs"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

type Apply struct {
	cmd *cobra.Command

	mainopts *Options
	files    []string
}

func NewApply(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply <options>",
		Short: "apply objects to database",
	}
	TweakCommand(cmd)

	c := &Apply{
		cmd:      cmd,
		mainopts: opts,
	}
	c.cmd.RunE = func(cmd *cobra.Command, args []string) error { return c.Run(args) }
	flags := cmd.Flags()
	flags.StringSliceVarP(&c.files, "file", "f", nil, "resource file")

	return cmd
}

func (c *Apply) Run(args []string) error {
	var cmderr error

	if len(args) != 0 {
		return fmt.Errorf("no arguments expected")
	}

	for _, fp := range c.files {
		files, err := glob.Glob(c.mainopts.fs, fp)
		if err != nil {
			fmt.Fprintf(c.cmd.ErrOrStderr(), "%q: %s\n", fp, err.Error())
			cmderr = fmt.Errorf("apply failed for some resources")
			continue
		}
		for _, f := range files {
			var data []byte
			var err error
			multi := true

			if f == "-" {
				data, err = io.ReadAll(c.cmd.InOrStdin())
			} else {
				data, err = vfs.ReadFile(c.mainopts.fs, f)
			}
			if err != nil {
				fmt.Fprintf(c.cmd.ErrOrStderr(), "cannot read file %q: %s\n", f, err.Error())
				cmderr = fmt.Errorf("apply failed for some resources")
				continue
			}

			var list List

			var m map[string]interface{}
			err = yaml.Unmarshal(data, &m)
			if err != nil {
				fmt.Fprintf(c.cmd.ErrOrStderr(), "cannot unmarshal file %q: %s\n", f, err.Error())
				cmderr = fmt.Errorf("apply failed for some resources")
				continue
			}

			if !isList(m) {
				u, err := db.UnstructuredFor(m)
				if err != nil {
					fmt.Fprintf(c.cmd.ErrOrStderr(), "cannot unmarshal file %q: %s\n", f, err.Error())
					cmderr = fmt.Errorf("apply failed for some resources")
					continue
				}
				list.Items = []Object{u}
				multi = false
			} else {
				err := yaml.Unmarshal(data, &list)
				if err != nil {
					fmt.Fprintf(c.cmd.ErrOrStderr(), "cannot unmarshal file %q: %s\n", f, err.Error())
					cmderr = fmt.Errorf("apply failed for some resources")
					continue
				}
			}

			for i, o := range list.Items {
				var err error
				switch {
				case !filesystem.CheckType(o.GetType()):
					err = fmt.Errorf("invalid resource type %q", o.GetType())
				case !filesystem.CheckNamespace(o.GetNamespace()):
					err = fmt.Errorf("invalid namespace %q", o.GetNamespace())
				case !filesystem.CheckName(o.GetName()):
					err = fmt.Errorf("invalid resource name %q", o.GetName())
				}
				if err != nil {
					cmderr = IndexError(c.cmd, multi, i, f, "invalid resource meta", err)
					continue
				}

				cur, err := GetObject(c.mainopts, o)
				if err == nil {
					if cur.Status != nil {
						o.Status = cur.Status
					} else {
						o.Status = nil
					}

					if cur.GetGeneration() != 0 {
						o.SetGeneration(cur.GetGeneration())
					} else {
						o.SetGeneration(0)
					}
				}

				data, err := json.Marshal(o)
				if err != nil {
					cmderr = IndexError(c.cmd, multi, i, f, "cannot marshal manifest", err)
					continue
				}

				post, err := http.Post(c.mainopts.GetURL()+path.Join(o.GetType(), o.GetNamespace(), o.GetName()), "application/json", bytes.NewReader(data))
				if err != nil {
					cmderr = IndexError(c.cmd, multi, i, f, fmt.Sprintf("%s: post failed", database.NewObjectRefFor(o)), err)
					continue
				}
				_, err = ResponseData(post)
				if err != nil {
					cmderr = IndexError(c.cmd, multi, i, f, fmt.Sprintf("%s: cannot apply ", database.NewObjectRefFor(o)), err)
					continue
				}
				s := "updated"
				if post.StatusCode == http.StatusCreated {
					s = "created"
				}
				fmt.Fprintf(c.cmd.OutOrStdout(), "%s: %s\n", database.NewObjectRefFor(o), s)
			}
		}
	}
	return cmderr
}

func isList(m map[string]interface{}) bool {
	if len(m) != 1 || m["items"] == nil {
		return false
	}

	if l, ok := m["items"].([]interface{}); !ok {
		return false
	} else {
		for _, e := range l {
			if _, ok := e.(map[string]interface{}); !ok {
				return false
			}
		}
	}
	return true
}

func IndexError(c *cobra.Command, multi bool, index int, file string, msg string, err error) error {
	if multi {
		fmt.Fprintf(c.ErrOrStderr(), "%s for resource %d in %q: %s\n", msg, index+1, file, err.Error())
	} else {
		fmt.Fprintf(c.ErrOrStderr(), "%s for %q: %s\n", msg, file, err.Error())
	}
	return fmt.Errorf("apply failed for some resources")
}
