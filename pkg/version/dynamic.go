package version

type NodeInfoFunc func() (version string, links []Node)

type DynamicNode interface {
	Node
	SetFixed(bool)
}

type dynamicNode struct {
	node
	info  NodeInfoFunc
	fixed bool
}

func NewDynamicNode(typ, name string, info NodeInfoFunc) DynamicNode {
	return &dynamicNode{node: node{typ: typ, name: name}, info: info}
}

func (d *dynamicNode) GetVersion() string {
	var links []Node

	version := d.version
	if version == "" {
		version, links = d.info()
		if d.fixed {
			d.version = version
			d.links = links
		}
	}
	return version
}

func (d *dynamicNode) GetLinks() []Node {

	version := d.version
	links := d.links
	if version == "" {
		version, links = d.info()
		if d.fixed {
			d.version = version
			d.links = links
		}
	}
	return links
}

func (d *dynamicNode) SetFixed(b bool) {
	if !b {
		d.version = ""
	}
	d.fixed = b
}
