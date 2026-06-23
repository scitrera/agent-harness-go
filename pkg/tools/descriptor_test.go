package tools

import (
	"context"
	"encoding/json"
	"testing"
)

func TestDescribeAndDescriptors(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("x", HandlerFunc(func(context.Context, Request) (Result, error) { return Result{}, nil })); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register("y", HandlerFunc(func(context.Context, Request) (Result, error) { return Result{}, nil })); err != nil {
		t.Fatalf("register: %v", err)
	}
	r.Describe(Descriptor{Name: "x", Description: "the x tool", Parameters: json.RawMessage(`{"type":"object"}`)})

	got := map[string]Descriptor{}
	for _, d := range r.Descriptors() {
		got[d.Name] = d
	}
	if got["x"].Description != "the x tool" {
		t.Fatalf("x descriptor: %+v", got["x"])
	}
	// y was registered without a descriptor -> name-only entry so the model
	// still learns it exists.
	if _, ok := got["y"]; !ok {
		t.Fatalf("y should have a name-only descriptor: %+v", got)
	}
}

func TestLocalDescriptorsCoverAllLocalTools(t *testing.T) {
	byName := map[string]Descriptor{}
	for _, d := range localDescriptors() {
		byName[d.Name] = d
	}
	for _, name := range []string{"read_file", "write_file", "edit_file", "list_dir", "inspect_file", "shell", "python", "web_search"} {
		d, ok := byName[name]
		if !ok {
			t.Fatalf("missing descriptor for %s", name)
		}
		if d.Description == "" {
			t.Fatalf("%s missing description", name)
		}
		var schema map[string]any
		if err := json.Unmarshal(d.Parameters, &schema); err != nil {
			t.Fatalf("%s parameters not valid JSON: %v", name, err)
		}
		if schema["type"] != "object" {
			t.Fatalf("%s parameters not an object schema: %v", name, schema)
		}
	}
}
