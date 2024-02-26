package version

import (
	"fmt"
	"io"
	"slices"
	"unicode/utf8"

	"github.com/mandelsoft/engine/pkg/scanner"
	"github.com/mandelsoft/engine/pkg/utils"
)

type parsedNode struct {
	id      Id
	version string
	links   []*parsedNode
}

var _ Node = (*parsedNode)(nil)

func NewParsedNode(typ any, name, version string) *parsedNode {
	return &parsedNode{id: NewId(typ, name), version: version}
}

func (n *parsedNode) GetId() Id {
	return n.id
}

func (n *parsedNode) GetName() string {
	return n.id.GetName()
}

func (n *parsedNode) GetType() string {
	return n.id.GetType()
}

func (n *parsedNode) GetVersion() string {
	return n.version
}

func (n *parsedNode) GetLinks() []Id {
	return utils.TransformSlice(n.links, func(n *parsedNode) Id { return n.GetId() })
}

func (n *parsedNode) GetNodeLinks() []*parsedNode {
	return slices.Clone(n.links)
}

func (n *parsedNode) AddDep(d *parsedNode) {
	var i int

	for i = 0; i < len(n.links); i++ {
		if CompareId(n.links[i], d) > 0 {
			break
		}
	}

	n.links = append(append(n.links[:i], d), n.links[i:]...)
	slices.SortFunc(n.links, compareParsedNode)
}

func (n *parsedNode) AsGraph() (GraphView, error) {
	g := NewGraph()

	err := n.addTo(g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (n *parsedNode) addTo(g Graph) error {
	o := g.GetNode(n)
	if o != nil {
		if o.GetVersion() != n.GetVersion() {
			return fmt.Errorf("inconsistent version form node %q: %s != %s", n.id, n.GetVersion(), o.GetVersion())
		}
	} else {
		g.AddNode(n)
		for _, d := range n.links {
			err := d.addTo(g)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *parsedNode) Dump(w io.Writer) error {
	return n.dump(w, "")
}

func (n *parsedNode) dump(w io.Writer, gap string) error {
	_, err := fmt.Fprintf(w, "%s%s", gap, n.id)
	if err != nil {
		return err
	}
	if n.version != "" {
		_, err := fmt.Fprintf(w, "[%s]", n.version)
		if err != nil {
			return err
		}
	}

	if len(n.links) > 0 {
		_, err := fmt.Fprintf(w, " (")
		if err != nil {
			return err
		}
		for i, l := range n.links {
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
			err = l.dump(w, gap+"  ")
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

func compareParsedNode(a, b *parsedNode) int {
	return CompareId(a, b)
}

////////////////////////////////////////////////////////////////////////////////

type parser struct {
	scanner.Scanner
}

func NewParser(in string) *parser {
	p := &parser{
		Scanner: scanner.NewScanner(in),
	}
	return p
}

func isSpecial(r rune) bool {
	switch r {
	case '(', ')', '[', ']', '/':
		return true
	case utf8.RuneError:
		return true
	default:
		return false
	}
}

func (s *parser) parseSegment() (string, error) {
	seg := ""

	for !isSpecial(s.Current()) {
		seg += string(s.Current())
		s.Next()
	}
	if seg == "" {
		return "", s.Errorf("name segment expected")
	}
	return seg, nil
}

func (s *parser) parseName() (string, error) {
	name := ""
	for {
		seg, err := s.parseSegment()
		if err != nil {
			return "", err
		}
		if s.Current() != '/' {
			if name != "" {
				name += "/"
			}
			name += seg
			break
		}
	}
	if name == "" {
		return "", s.Errorf("name expected")
	}
	return name, nil
}

func (s *parser) parseVersion() (string, error) {
	if s.Current() != '[' {
		return "", nil
	}
	s.Next()
	seg, err := s.parseSegment()
	if err != nil {
		return "", err
	}
	err = s.ConsumeRune(']')
	if err != nil {
		return "", err
	}
	return seg, nil
}

func (s *parser) parseEffName() (string, string, error) {
	typ, err := s.parseSegment()
	if err != nil {
		return "", "", err
	}
	err = s.ConsumeRune('/')
	if err != nil {
		return "", "", err
	}
	name, err := s.parseName()
	if err != nil {
		return "", "", err
	}
	return typ, name, nil
}

func (s *parser) parseGraph() (*parsedNode, error) {
	typ, name, err := s.parseEffName()
	if err != nil {
		return nil, err
	}

	v, err := s.parseVersion()
	if err != nil {
		return nil, err
	}
	n := NewParsedNode(typ, name, v)

	if s.Current() == '(' {
		for {
			s.Next()
			g, err := s.parseGraph()
			if err != nil {
				return nil, err
			}
			n.AddDep(g)
			if s.Current() != ',' {
				break
			}
		}
		err = s.ConsumeRune(')')
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

func Parse(in string) (*parsedNode, error) {
	p := NewParser(in)

	g, err := p.parseGraph()
	if err != nil {
		return nil, err
	}
	if p.Current() != 0 {
		return nil, p.Errorf("unexpected character %q", string(p.Current()))
	}
	return g, nil
}
