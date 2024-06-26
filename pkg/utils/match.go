/*
 * SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company and Gardener contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"fmt"
	"strings"
)

type Matcher interface {
	Match(s string) bool
	String() string
}

type glob struct {
	pattern string
	runes   []interface{}
}

func NewStringGlobMatcher(pattern string) Matcher {
	return &glob{pattern, Runes(pattern)}
}

func NewStringMatcher(s string) Matcher {
	return StringMatcher(s)
}

type StringMatcher string

func (m StringMatcher) Match(s string) bool {
	return string(m) == s
}

func (m StringMatcher) String() string {
	return string(m)
}

func (g *glob) String() string {
	return g.pattern
}

func (g *glob) Match(s string) bool {
	return Match(Runes(s), g.runes, '*', RuneMatcher)
}

////////////////////////////////////////////////////////////////////////////////

type pathGlob struct {
	pattern    string
	components []interface{}
}

func NewPathGlobMatcher(pattern string) Matcher {
	var globs []interface{}

	for _, p := range path_comps(pattern) {
		if p == "**" {
			globs = append(globs, p)
		} else {
			globs = append(globs, NewStringGlobMatcher(p.(string)))
		}
	}
	return &pathGlob{pattern, globs}
}

func (g *pathGlob) String() string {
	return g.pattern
}

func path_comps(s string) (comps []interface{}) {
	for _, comp := range strings.Split(s, "/") {
		if comp != "" {
			comps = append(comps, comp)
		}
	}
	return
}

func (g *pathGlob) Match(s string) bool {
	fmt.Printf("Match %s %s\n", s, g)
	return Match(path_comps(s), g.components, "**", GlobMatcher)
}

////////////////////////////////////////////////////////////////////////////////

func Runes(s string) (runes []interface{}) {
	for _, rune := range s {
		runes = append(runes, rune)
	}
	return
}

func RuneMatcher(s, p interface{}) bool {
	return s == p || p == '?'
}

func GlobMatcher(s, p interface{}) bool {
	return s == p || p.(Matcher).Match(s.(string))
}

func Match(s, p []interface{}, star interface{}, match func(interface{}, interface{}) bool) bool {
	for i, c := range s {
		if i >= len(p) {
			return false
		}
		switch p[i] {
		case c:
		case star:
			rest := p[i+1:]
			for i <= len(s) {
				if Match(s[i:], rest, star, match) {
					return true
				}
				i++
			}
			return false
		default:
			if !match(c, p[i]) {
				return false
			}
		}
	}
	return len(s) == len(p)
}
