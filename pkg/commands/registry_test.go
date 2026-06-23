package commands

import "testing"

func TestRegistryLookupAndList(t *testing.T) {
	reg := New([]Command{
		{Name: "commit", Description: "c"},
		{Name: "dock-discord", Description: "d"},
		{Name: "commit", Description: "dup ignored"},
	})
	if reg.Len() != 2 {
		t.Fatalf("expected 2 commands (dup folded), got %d", reg.Len())
	}
	if c, ok := reg.Lookup("COMMIT"); !ok || c.Description != "c" {
		t.Fatalf("case-insensitive lookup failed: %#v ok=%v", c, ok)
	}
	if _, ok := reg.Lookup("dock_discord"); !ok {
		t.Fatal("underscore variant should resolve to hyphen command")
	}
	if _, ok := reg.Lookup("nope"); ok {
		t.Fatal("unknown command should not resolve")
	}
	list := reg.List()
	if len(list) != 2 || list[0].Name != "commit" || list[1].Name != "dock-discord" {
		t.Fatalf("list not sorted by name: %#v", list)
	}
}

func TestNilRegistry(t *testing.T) {
	var reg *Registry
	if _, ok := reg.Lookup("x"); ok {
		t.Fatal("nil registry lookup should be false")
	}
	if reg.List() != nil || reg.Len() != 0 {
		t.Fatal("nil registry should be empty")
	}
}
