package requirements

import "strings"

type Source struct {
	Label string `json:"label"`
	Kind  string `json:"kind,omitempty"`
	Path  string `json:"path,omitempty"`
}

func NewSource(label string) Source {
	return Source{Label: strings.TrimSpace(label)}
}

func (s Source) display() string {
	if strings.TrimSpace(s.Label) != "" {
		return strings.TrimSpace(s.Label)
	}
	if strings.TrimSpace(s.Path) != "" {
		return strings.TrimSpace(s.Path)
	}
	if strings.TrimSpace(s.Kind) != "" {
		return strings.TrimSpace(s.Kind)
	}
	return "<missing source>"
}

func (s Source) valid() bool {
	return strings.TrimSpace(s.Label) != ""
}

type Sourced[T any] struct {
	Value  T      `json:"value"`
	Source Source `json:"source"`
}

func withSource[T any](value T, source Source) Sourced[T] {
	return Sourced[T]{Value: value, Source: source}
}
