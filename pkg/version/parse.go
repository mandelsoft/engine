package version

import (
	"fmt"
	"unicode/utf8"
)

type parser struct {
	in      []byte
	offset  int
	no      int
	current rune

	nodes map[string]*node
}

func NewParser(in string) *parser {
	p := &parser{
		in:    []byte(in),
		nodes: map[string]*node{},
	}
	p.Next()
	return p
}

func (s *parser) Next() rune {
	if s.offset >= len(s.in) {
		s.current = 0
		return 0
	}
	r, size := utf8.DecodeRune(s.in[s.offset:])
	s.current = r
	if r == utf8.RuneError {
		return r
	}
	s.offset += size
	s.no++
	return r
}

func (s *parser) parse(r rune) error {
	if s.Current() != r {
		return s.Errorf("%q expected", string(r))
	}
	s.Next()
	return nil
}

func (s *parser) Current() rune {
	return s.current
}

func (s *parser) Position() int {
	return s.no
}

func (s *parser) Errorf(msg string, args ...interface{}) error {
	return fmt.Errorf("%q %d: %s", string(s.in), s.Position(), fmt.Sprintf(msg, args...))
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
	err = s.parse(']')
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
	err = s.parse('/')
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
	s.nodes[n.GetEffName()] = n

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
		err = s.parse(')')
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
		n := s.nodes[t.GetEffName()]
		if n == nil {
			return fmt.Errorf("node %q not in graph", t.GetEffName())
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
	err = p.parse(':')
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
