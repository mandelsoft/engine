package version

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mandelsoft/engine/pkg/utils"
)

type Id interface {
	GetName() string
	GetType() string
}

type id struct {
	name string
	typ  string
}

func NewId(typ string, name string) Id {
	return id{
		typ:  typ,
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

func NewNode(typ, name, version string, deps ...Id) ConfigurableNode {
	deps = slices.Clone(deps)

	slices.SortFunc(deps, CompareId)
	return &node{NewId(typ, name), version, deps}
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
	return g.nodes[id]
}

func (g *graph) Nodes() []Id {
	return utils.MapKeys(g.nodes, CompareId)
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
		compose:    utils.OptionalDefaulted[Composer](Composed, cmps...),
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
	if c := cycle(id, stack...); c != nil {
		return "", fmt.Errorf("dependency cycle %s", utils.JoinFunc(c, "->", GetEffName))
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

func cycle(id Id, stack ...Id) []Id {
	i := slices.Index(stack, id)
	if i < 0 {
		return nil
	}
	return append(slices.Clone(stack[i:]), id)
}
