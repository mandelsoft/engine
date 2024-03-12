package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/spf13/cobra"
)

type Delete struct {
	cmd *cobra.Command

	mainopts *Options
	force    bool
	all      bool
	filemode bool
}

func NewDelete(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <type> {<object>} <options>",
		Short: "delete objects from database",
	}
	TweakCommand(cmd)

	c := &Delete{
		cmd:      cmd,
		mainopts: opts,
	}
	c.cmd.RunE = func(cmd *cobra.Command, args []string) error { return c.Run(args) }
	flags := cmd.Flags()
	flags.BoolVarP(&c.force, "force", "F", false, "force mode")
	flags.BoolVarP(&c.all, "all", "A", false, "all objects")
	flags.BoolVarP(&c.filemode, "file", "f", false, "manifest files")
	return cmd
}

func (c *Delete) Run(args []string) error {
	var cmderr error

	if len(args) < 1 && !c.filemode {
		return fmt.Errorf("object type required")
	}

	handler := func(f string, list ...database.ObjectId) error {
		var cmderr error
		multi := len(list) > 1
		for i, o := range list {
			req, err := http.NewRequest("DELETE", c.mainopts.GetURL()+path.Join(o.GetType(), o.GetNamespace(), o.GetName()), nil)
			if err != nil {
				cmderr = IndexError(c.cmd, multi, i+1, database.StringId(o), "deletion failed", err)
				continue
			}
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				cmderr = IndexError(c.cmd, multi, i+1, database.StringId(o), "request failed", err)
				continue
			}
			if r.StatusCode == http.StatusOK {
				fmt.Fprintf(c.cmd.OutOrStdout(), "%s: deleted\n", database.StringId(o))
			} else {
				if c.force {
					obj, err := GetObject(c.mainopts, o)
					if err != nil {
						if !errors.Is(err, database.ErrNotExist) {
							cmderr = IndexError(c.cmd, multi, i+1, database.StringId(obj), "cannot remove finalizers", err)
							continue
						}
					} else {
						obj.SetFinalizers(nil)
						data, err := json.Marshal(obj)
						if err != nil {
							cmderr = IndexError(c.cmd, multi, i+1, database.StringId(obj), "cannot remove finalizers", err)
							continue
						}

						post, err := http.Post(c.mainopts.GetURL()+path.Join(obj.GetType(), obj.GetNamespace(), obj.GetName()), "application/json", bytes.NewReader(data))
						if err != nil {
							cmderr = IndexError(c.cmd, multi, i+1, database.StringId(obj), "cannot remove finalizers", err)
							continue
						}
						_, err = ResponseData(post)
						if err != nil {
							cmderr = IndexError(c.cmd, multi, i+1, database.StringId(obj), "cannot remove finalizers", err)
							continue
						}
						fmt.Fprintf(c.cmd.OutOrStdout(), "%s: deletion enforced\n", database.StringId(o))
					}
				} else {
					fmt.Fprintf(c.cmd.OutOrStdout(), "%s: deletion requested\n", database.StringId(o))
				}
			}
		}
		return cmderr
	}

	if !c.filemode {
		typ := args[0]
		if typ == "" {
			return fmt.Errorf("non-empty type required")
		}

		var list []database.ObjectId

		if len(args) > 1 {
			for _, arg := range args[1:] {
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

				list = append(list, database.NewObjectId(typ, ns, arg))
			}
		} else {
			if !c.all {
				return fmt.Errorf("no object specified")
			}
			ns := c.mainopts.namespace
			if ns == "" {
				ns = "*"
			}

			req, err := http.NewRequest("LIST", c.mainopts.GetURL()+path.Join(typ, ns), nil)
			if err != nil {
				return err
			}
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			data, err := ResponseData(r)
			if err != nil {
				return fmt.Errorf("get failed with status code %s", r.Status)
			}
			var l List
			err = json.Unmarshal(data, &l)
			if err != nil {
				return err
			}
			list = append(list, utils.TransformSlice(l.Items, ObjectIdFor)...)
		}
		for _, o := range list {
			err := handler("", o)
			if err != nil {
				cmderr = err
			}
		}
	}

	if c.filemode {
		HandleObjects(c.cmd, c.mainopts, args, func(f string, items ...Object) error {
			list := utils.TransformSlice(items, ObjectIdFor)
			return handler(f, list...)
		})
	}
	return cmderr
}

func ObjectIdFor(o Object) database.ObjectId {
	return database.NewObjectIdFor(o)
}
