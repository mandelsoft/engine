package version

import (
	"fmt"
	"unicode/utf8"

	"github.com/mandelsoft/engine/pkg/scanner"
)

type parser struct {
	scanner.Scanner

	nodes map[string]*node
}

func NewParser(in string) *parser {
	p := &parser{
		Scanner: scanner.NewScanner(in),
		nodes:   map[string]*node{},
	}
	return p
}

func isSpecial(r rune) bool {
	switch r {
	case '(', ')', '[', ']', '/', ':':
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

func (s *parser) parseGraph() (*node, error) {
	typ, name, err := s.parseEffName()
	if err != nil {
		return nil, err
	}
	if n := s.nodes[name]; n != nil {
		if s.Current() == '(' {
			return nil, s.Errorf("( not possible for already known node %q", name)
		}
		return n, nil
	}

	n := NewNode(typ, name, "")
	s.nodes[GetEffName(n)] = n

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

func (s *parser) parseVersionList() error {
	for {
		typ, name, err := s.parseEffName()
		if err != nil {
			return err
		}
		v, err := s.parseVersion()
		if err != nil {
			return err
		}

		t := NewNode(typ, name, v)
		n := s.nodes[GetEffName(t)]
		if n == nil {
			return fmt.Errorf("node %q not in graph", GetEffName(t))
		}
		n.version = v

		if s.Current() != ',' {
			return nil
		}
		s.Next()
	}
}

func Parse(in string) (Node, error) {
	p := NewParser(in)

	g, err := p.parseGraph()
	if err != nil {
		return nil, err
	}
	err = p.ConsumeRune(':')
	if err != nil {
		return nil, err
	}
	err = p.parseVersionList()
	if err != nil {
		return nil, err
	}
	if p.Current() != 0 {
		return nil, p.Errorf("unexpected character %q", string(p.Current()))
	}
	return g, nil
}
