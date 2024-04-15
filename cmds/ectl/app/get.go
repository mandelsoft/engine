package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

type Get struct {
	cmd *cobra.Command

	mainopts *Options
	sort     string
	output   string
	closure  bool
}

func NewGet(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <type> {<object>} <options>",
		Short: "get objects from database",
	}
	TweakCommand(cmd)

	c := &Get{
		cmd:      cmd,
		mainopts: opts,
	}
	c.cmd.RunE = func(cmd *cobra.Command, args []string) error { return c.Run(args) }
	flags := cmd.Flags()
	flags.StringVarP(&c.sort, "sort", "s", "", "sort field")
	flags.StringVarP(&c.output, "output", "o", "", "output format")
	flags.BoolVarP(&c.closure, "closure", "c", false, "namespace closure")
	return cmd
}

func (c *Get) Run(args []string) error {

	if len(args) == 0 {
		args = []string{"*"}
	}

	t := args[0]
	if t == "" {
		return fmt.Errorf("non-empty type required")
	}
	if strings.Contains(t, "/") {
		return fmt.Errorf("invalid / in type name")
	}
	typlist := sliceutils.Transform(strings.Split(t, ","), strings.TrimSpace)

	var list []Object
	useList := len(args) > 2

	if len(args) > 1 {
		for _, arg := range args[1:] {
			orig := arg
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

			for _, typ := range typlist {
				get, err := http.Get(c.mainopts.GetURL() + path.Join(typ, ns, arg))
				if err != nil {
					return fmt.Errorf("%s: %w", orig, err)
				}
				data, err := ResponseData(get)
				if err != nil {
					return fmt.Errorf("%s: %w", orig, err)
				}

				var o Object
				err = json.Unmarshal(data, &o)
				if err != nil {
					return fmt.Errorf("%s: %w", orig, err)
				}
				list = append(list, o)
			}
		}
	} else {
		useList = true
		ns := c.mainopts.namespace
		if c.closure {
			ns += "*"
		}

		for _, typ := range typlist {
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
			list = append(list, l.Items...)
		}
	}

	slices.SortFunc(list, database.CompareObject[Object])

	var elems interface{}

	if useList {
		elems = &List{
			Items: list,
		}
	} else {
		if len(list) > 0 {
			elems = list[0]
		}
	}

	switch strings.ToLower(strings.TrimSpace(c.output)) {
	case "":
		return PrintObjectList(c.cmd.OutOrStdout(), list, RequireTypeField(list), c.sort)
	case "json":
		data, err := json.Marshal(elems)
		if err != nil {
			return err
		}
		fmt.Fprintf(c.cmd.OutOrStdout(), "%s\n", string(data))
	case "yaml":
		data, err := yaml.Marshal(elems)
		if err != nil {
			return err
		}
		fmt.Fprintf(c.cmd.OutOrStdout(), "%s\n", string(data))
	}
	return nil
}

func PrintObjectList(w io.Writer, list []Object, typeField bool, sortField string) error {
	if len(list) == 0 {
		fmt.Fprintf(w, "no resource found\n")
		return nil
	}
	fieldList := MapFields(list, typeField)
	var columnList []string
	if typeField {
		columnList = []string{"NAMESPACE", "NAME", "TYPE", "STATUS"}
	} else {
		columnList = []string{"NAMESPACE", "NAME", "STATUS"}
	}

	sortField = strings.ToUpper(strings.TrimSpace(sortField))
	sort := -1
	if sortField != "" {
		for i, n := range columnList {
			if n == sortField {
				sort = i
				break
			}
		}
		if sort < 0 {
			return fmt.Errorf("unknown sort field %q", sortField)
		}
	}
	if sort >= 0 {
		slices.SortFunc(fieldList, func(a, b []string) int { return strings.Compare(a[sort], b[sort]) })
	}
	max := make([]int, len(columnList), len(columnList))
	for i, s := range columnList {
		max[i] = len(s)
	}
	for _, cols := range fieldList {
		for i, s := range cols {
			if max[i] < len(s) {
				max[i] = len(s)
			}
		}
	}

	f := formatString(max)
	printLine(w, columnList, f)
	for _, cols := range fieldList {
		printLine(w, cols, f)
	}
	return nil
}

func printLine(w io.Writer, cols []string, msg string) {
	fmt.Fprintf(w, "%s\n", strings.TrimRight(fmt.Sprintf(msg, sliceutils.Convert[any](cols)...), " "))
}

func formatString(max []int) string {
	msg := ""
	for _, l := range max {
		msg += fmt.Sprintf("%%-%ds ", l)
	}
	return msg[:len(msg)-1]
}

func MapFields(list []Object, typeField bool) [][]string {
	var r [][]string
	for _, o := range list {
		var l []string

		s := o.GetStatusValue()
		if o.IsDeleting() {
			if s == "" {
				s = "<deleting>"
			} else {
				s += ",<deleting>"
			}
		}
		if typeField {
			l = []string{
				o.GetNamespace(), o.GetName(), o.GetType(), s,
			}
		} else {
			l = []string{
				o.GetNamespace(), o.GetName(), s,
			}
		}
		r = append(r, l)
	}
	return r
}

func RequireTypeField(list []Object) bool {
	if len(list) < 2 {
		return false
	}
	t := list[0].GetType()
	for _, o := range list {
		if o.GetType() != t {
			return true
		}
	}
	return false
}

func GetObject(opts *Options, id database.ObjectId) (Object, error) {
	get, err := http.Get(opts.GetURL() + path.Join(id.GetType(), id.GetNamespace(), id.GetName()))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", database.NewObjectRefFor(id), err)
	}
	data, err := ResponseData(get)

	var o Object
	err = json.Unmarshal(data, &o)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", database.NewObjectRefFor(id), err)
	}
	return o, nil
}
