package version

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/stringutils"
)

type Id interface {
	GetName() string
	GetType() string
}

type id struct {
	name string
	typ  string
}

func NewId(typ any, name string) Id {
	return id{
		typ:  fmt.Sprintf("%s", typ),
		name: name,
	}
}

func NewIdFor(o Id) Id {
	return id{
		typ:  o.GetType(),
		name: o.GetName(),
	}
}

func (i id) GetName() string {
	return i.name
}

func (i id) GetType() string {
	return i.typ
}

func (i id) String() string {
	return GetEffName(i)
}

func CompareId(a, b Id) int {
	i := strings.Compare(a.GetName(), b.GetName())
	if i != 0 {
		return i
	}
	return strings.Compare(a.GetType(), b.GetType())
}

////////////////////////////////////////////////////////////////////////////////

type Node interface {
	GetId() Id
	GetLinks() []Id

	GetVersion() string
}

type ConfigurableNode interface {
	Node
	AddDep(id Id)
}

type node struct {
	id      Id
	version string
	links   []Id
}

var _ ConfigurableNode = (*node)(nil)

func NewNode(typ any, name, version string, deps ...Id) ConfigurableNode {
	deps = slices.Clone(deps)

	slices.SortFunc(deps, CompareId)
	return &node{NewId(typ, name), version, deps}
}

func NewNodeById(id Id, version string, deps ...Id) ConfigurableNode {
	deps = slices.Clone(deps)

	slices.SortFunc(deps, CompareId)
	return &node{NewIdFor(id), version, deps}
}

func (n *node) GetId() Id {
	return n.id
}

func (n *node) GetName() string {
	return n.id.GetName()
}

func (n *node) GetType() string {
	return n.id.GetType()
}

func (n *node) GetVersion() string {
	return n.version
}

func (n *node) GetLinks() []Id {
	return slices.Clone(n.links)
}

func (n *node) AddDep(d Id) {
	var i int

	for i = 0; i < len(n.links); i++ {
		if CompareId(n.links[i], d) > 0 {
			break
		}
	}

	n.links = append(append(n.links[:i], NewIdFor(d)), n.links[i:]...)
}

func GetEffName(id Id) string {
	return fmt.Sprintf("%s/%s", id.GetType(), id.GetName())
}

func GetVersionedName(n Node) string {
	v := n.GetVersion()
	if v == "" {
		return GetEffName(n.GetId())
	}
	return fmt.Sprintf("%s[%s]", GetEffName(n.GetId()), v)
}

////////////////////////////////////////////////////////////////////////////////

type GraphView interface {
	GetNode(id Id) Node
	Nodes() []Id
	Leaves() []Id
	Roots() []Id
	Dump(w io.Writer) error
}

type Graph interface {
	GraphView

	AddNode(n Node)
}

type graph struct {
	nodes map[Id]Node
}

func NewGraph() Graph {
	return &graph{nodes: map[Id]Node{}}
}

func (g *graph) AddNode(n Node) {
	g.nodes[n.GetId()] = n
}

func (g *graph) GetNode(id Id) Node {
	return g.nodes[NewIdFor(id)]
}

func (g *graph) Nodes() []Id {
	return maputils.Keys(g.nodes, CompareId)
}

////////////////////////////////////////////////////////////////////////////////

type EvaluatedGraph interface {
	GraphView
	FormalVersion(id Id) string
}

type _GraphView = GraphView

type evaluatedGraph struct {
	_GraphView
	compose  Composer
	versions map[Id]string
}

func EvaluateGraph(g GraphView, cmps ...Composer) (EvaluatedGraph, error) {
	e := &evaluatedGraph{
		_GraphView: g,
		compose:    general.OptionalDefaulted[Composer](Composed, cmps...),
		versions:   map[Id]string{},
	}

	for _, n := range g.Nodes() {
		_, err := e.getVersion(n)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *evaluatedGraph) FormalVersion(id Id) string {
	return e.versions[id]
}

////////////////////////////////////////////////////////////////////////////////

func (e *evaluatedGraph) getVersion(id Id, stack ...Id) (string, error) {
	if c := general.Cycle(id, stack...); c != nil {
		return "", fmt.Errorf("dependency cycle %s", stringutils.JoinFunc(c, "->", GetEffName))
	}
	n := e.GetNode(id)
	if n == nil {
		if len(stack) == 0 {
			return "", fmt.Errorf("unknown node %q", id)
		}
		return "", fmt.Errorf("unknown node %q used in %q", id, stack[len(stack)-1])
	}
	if v, ok := e.versions[id]; ok {
		return v, nil
	}
	links := n.GetLinks()
	if len(links) == 0 {
		e.versions[id] = e.compose.Compose(n)
		return e.versions[id], nil
	}

	var graphs []string
	slices.SortFunc(links, CompareId)
	for _, d := range links {
		g, err := e.getVersion(d, append(slices.Clone(stack), id)...)
		if err != nil {
			return "", err
		}
		graphs = append(graphs, g)
	}
	e.versions[id] = e.compose.Compose(n, graphs...)
	return e.versions[id], nil
}

////////////////////////////////////////////////////////////////////////////////

func (g *graph) Leaves() []Id {
	var r []Id
	for id, n := range g.nodes {
		if len(n.GetLinks()) == 0 {
			r = append(r, id)
		}
	}
	return r
}

func (g *graph) Roots() []Id {
	var r []Id
outer:
	for id, n := range g.nodes {
		for _, t := range g.nodes {
			for _, d := range t.GetLinks() {
				if d == n.GetId() {
					continue outer
				}
			}
		}
		r = append(r, id)
	}
	slices.SortFunc(r, CompareId)
	return r
}

func (g *graph) Dump(w io.Writer) error {
	for i, r := range g.Roots() {
		if i > 0 {
			_, err := fmt.Fprintf(w, "\n")
			if err != nil {
				return err
			}
		}
		err := g.dump(w, r, "")
		if err != nil {
			return err
		}
	}
	return g.dumpVersions(w)
}

func (g *graph) dumpVersions(w io.Writer) error {
	for _, id := range g.Nodes() {
		n := g.nodes[id]
		if n.GetVersion() != "" {
			_, err := fmt.Fprintf(w, "\n%s[%s]", id, n.GetVersion())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *graph) dump(w io.Writer, id Id, gap string) error {
	n := g.nodes[NewIdFor(id)]
	if n == nil {
		_, err := fmt.Fprintf(w, "%s%s (unknown)", gap, n.GetId())
		return err
	}
	_, err := fmt.Fprintf(w, "%s%s", gap, n.GetId())
	if err != nil {
		return err
	}
	links := n.GetLinks()
	if len(links) > 0 {
		_, err := fmt.Fprintf(w, " (")
		if err != nil {
			return err
		}
		for i, l := range links {
			if i > 0 {
				_, err := fmt.Fprintf(w, ",")
				if err != nil {
					return err
				}
			}
			_, err := fmt.Fprintf(w, "\n")
			if err != nil {
				return err
			}
			err = g.dump(w, l, gap+"  ")
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(w, "\n%s)", gap)
		if err != nil {
			return err
		}
	}
	return nil
}
