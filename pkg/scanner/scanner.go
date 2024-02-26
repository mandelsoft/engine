package scanner

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

type Scanner interface {
	Next() rune
	ConsumeRune(r rune) error
	SkipBlanks() rune
	Current() rune
	Position() int

	Errorf(msg string, args ...interface{}) error
}

type scanner struct {
	in      []byte
	offset  int
	no      int
	current rune
}

func NewScanner(in string) Scanner {
	s := &scanner{
		in: []byte(in),
	}
	s.Next()
	return s
}

func (s *scanner) Next() rune {
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

func (s *scanner) ConsumeRune(r rune) error {
	if s.Current() != r {
		return s.Errorf("%q expected", string(r))
	}
	s.Next()
	return nil
}

func (s *scanner) Current() rune {
	return s.current
}

func (s *scanner) Position() int {
	return s.no
}

func (s *scanner) SkipBlanks() rune {
	n := s.Current()
	for unicode.IsSpace(n) {
		n = s.Next()
	}
	return n
}

func (s *scanner) Errorf(msg string, args ...interface{}) error {
	return fmt.Errorf("%q %d: %s", string(s.in), s.Position(), fmt.Sprintf(msg, args...))
}
