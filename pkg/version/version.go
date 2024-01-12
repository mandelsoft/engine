package version

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"
)

type Node interface {
	GetType() string
	GetName() string
	GetEffName() string
	GetVersionedName() string
	GetLinks() []Node

	AddDep(n Node)
}

type node struct {
	typ     string
	name    string
	version string
	links   []Node
}

var _ Node = (*node)(nil)

func NewNode(typ, name, version string) *node {
	return &node{typ, name, version, nil}
}

func (n *node) GetEffName() string {
	return fmt.Sprintf("%s/%s", n.typ, n.name)
}

func (n *node) GetVersionedName() string {
	if n.version == "" {
		return n.GetEffName()
	}
	return fmt.Sprintf("%s[%s]", n.GetEffName(), n.version)
}

func (n *node) GetName() string {
	return n.name
}

func (n *node) GetLinks() []Node {
	return slices.Clone(n.links)
}

func (n *node) GetType() string {
	return n.typ
}

func (n *node) GetVersion() string {
	return n.version
}

func (n *node) AddDep(d Node) {
	var i int

	for i = 0; i < len(n.links); i++ {
		if strings.Compare(n.links[i].GetEffName(), d.GetEffName()) > 0 {
			break
		}
	}

	n.links = append(append(n.links[:i], d), n.links[i:]...)
}

func getId(n Node, nodes map[string]Node) string {
	eff := n.GetEffName()
	if _, ok := nodes[eff]; ok {
		return eff
	}
	nodes[eff] = n
	links := n.GetLinks()
	if len(links) == 0 {
		return eff
	}

	var graphs []string
	sort.Slice(links, func(i, j int) bool { return strings.Compare(links[i].GetEffName(), links[j].GetEffName()) < 0 })
	for _, d := range links {
		g := getId(d, nodes)
		graphs = append(graphs, g)
	}
	return fmt.Sprintf("%s(%s)", n.GetEffName(), strings.Join(graphs, ","))
}

func GetId(n Node) string {
	var list []string

	nodes := map[string]Node{}
	g := getId(n, nodes)
	for _, c := range nodes {
		list = append(list, c.GetVersionedName())
	}
	sort.Strings(list)
	return g + ":" + strings.Join(list, ",")
}

func GetVersionHash(n Node) string {
	h := sha256.Sum256([]byte(GetId(n)))
	return hex.EncodeToString(h[:])
}

type Version interface {
	GetId() string
	GetHash() string
}

type VersionFunc func() string

func (v VersionFunc) GetId() string {
	return v()
}

func (v VersionFunc) GetHash() string {
	h := sha256.Sum256([]byte(v()))
	return hex.EncodeToString(h[:])
}

func NewVersion(v string) (Version, error) {
	n, err := Parse(v)
	if err != nil {
		return nil, err
	}
	return VersionFunc(func() string { return GetId(n) }), nil
}

func NewNodeVersion(n Node) Version {
	return VersionFunc(func() string { return GetId(n) })
}
