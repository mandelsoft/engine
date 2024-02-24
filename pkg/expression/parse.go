package expression

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/mandelsoft/engine/pkg/utils"
)

type parser struct {
	in      []byte
	offset  int
	no      int
	current rune
}

func NewParser(in string) *parser {
	p := &parser{
		in: []byte(in),
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

func (s *parser) ParseRune(r rune) error {
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

func (s *parser) SkipBlank() rune {
	n := s.Current()
	for unicode.IsSpace(n) {
		n = s.Next()
	}
	return n
}

////////////////////////////////////////////////////////////////////////////////

func (s *parser) parseExpression() (*Node, error) {
	return s.parseLevel0()
}

func (s *parser) parseOperand() (*Node, error) {
	n := s.SkipBlank()
	switch {
	case unicode.IsDigit(n) || n == '-':
		return s.parseNumber()
	case unicode.IsLetter(n):
		return s.parseName()
	case n == '(':
		s.Next()
		e, err := s.parseExpression()
		if err != nil {
			return nil, err
		}
		err = s.ParseRune(')')
		if err != nil {
			return nil, err
		}
		return e, nil
	default:
		return nil, s.Errorf("unexpected character %q for operand", string(n))
	}
}

func (s *parser) parseNumber() (*Node, error) {
	sign := 1
	n := s.SkipBlank()
	for n == '-' {
		sign = -sign
		n = s.Next()
	}

	if !unicode.IsDigit(n) {
		return nil, s.Errorf("nuḿber must be a sequence of digits, but found %q", string(n))
	}
	num := 0
	for unicode.IsDigit(n) {
		num = num*10 + int(n-rune('0'))
		n = s.Next()
	}
	return &Node{
		Value: utils.Pointer(num * sign),
	}, nil
}

func (s *parser) parseName() (*Node, error) {
	n := s.SkipBlank()
	if !unicode.IsLetter(n) {
		return nil, s.Errorf("variable name must start with letter, but found %q", string(n))
	}
	name := ""
	for {
		name = name + string(n)
		n = s.Next()
		if !unicode.IsDigit(n) && !unicode.IsLetter(n) {
			break
		}
	}
	return &Node{
		Name: name,
	}, nil
}

func (s *parser) parseLevel0() (*Node, error) {
	o1, err := s.parseLevel1()
	if err != nil {
		return nil, err
	}
	for {
		switch s.SkipBlank() {
		case '+', '-':
			op := s.Current()
			s.Next()
			o2, err := s.parseLevel1()
			if err != nil {
				return nil, err
			}
			o1 = &Node{
				Name:    string(op),
				Parents: []*Node{o1, o2},
				Value:   nil,
			}
		default:
			return o1, nil
		}
	}
}

func (s *parser) parseLevel1() (*Node, error) {
	o1, err := s.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		switch s.SkipBlank() {
		case '/', '*':
			op := s.Current()
			s.Next()
			o2, err := s.parseOperand()
			if err != nil {
				return nil, err
			}
			o1 = &Node{
				Name:    string(op),
				Parents: []*Node{o1, o2},
				Value:   nil,
			}
		default:
			return o1, nil
		}
	}
}

func Parse(in string) (*Node, error) {
	p := NewParser(in)

	n, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if p.Current() != 0 {
		return nil, p.Errorf("unexpected character %q", string(p.Current()))
	}
	return n, nil
}
