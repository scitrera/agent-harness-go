// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package model

import (
	"context"
	"testing"
)

func testRegistry() *Registry {
	return NewRegistry([]Model{
		{Name: "light", Capabilities: Capabilities{Tools: true}, Tier: "light"},
		{Name: "primary", Capabilities: Capabilities{Vision: true, Tools: true}, Tier: "primary"},
		{Name: "audio", Capabilities: Capabilities{Audio: true, Tools: true}},
	}, "light")
}

func TestRegistryBasics(t *testing.T) {
	r := testRegistry()
	if got := r.List(); len(got) != 3 || got[0].Name != "light" {
		t.Fatalf("List order: %+v", got)
	}
	if d, ok := r.Default(); !ok || d.Name != "light" {
		t.Fatalf("Default = %+v (%v)", d, ok)
	}
	if _, ok := r.Get("nope"); ok {
		t.Fatal("Get missing returned ok")
	}
	vis := r.Capable(Capabilities{Vision: true})
	if len(vis) != 1 || vis[0].Name != "primary" {
		t.Fatalf("Capable(vision) = %+v", vis)
	}
}

func TestCapabilityDefaultSelection(t *testing.T) {
	r := testRegistry()
	sel := CapabilityDefault{}
	ctx := context.Background()

	// No special capability needed → keep the default.
	got, _ := sel.SelectModel(ctx, SelectInput{Required: Capabilities{Tools: true}, Registry: r, Default: "light"})
	if got != "light" {
		t.Fatalf("plain turn want light, got %q", got)
	}

	// Vision required but default ("light") lacks it → fall back to a capable model.
	got, _ = sel.SelectModel(ctx, SelectInput{Required: Capabilities{Vision: true, Tools: true}, Registry: r, Default: "light"})
	if got != "primary" {
		t.Fatalf("vision turn want primary (capability fallback), got %q", got)
	}

	// Default already satisfies → keep it even when capable.
	got, _ = sel.SelectModel(ctx, SelectInput{Required: Capabilities{Vision: true, Tools: true}, Registry: r, Default: "primary"})
	if got != "primary" {
		t.Fatalf("capable default want primary, got %q", got)
	}

	// Nothing satisfies → best-effort fall back to the default (never errors).
	got, err := sel.SelectModel(ctx, SelectInput{Required: Capabilities{Audio: true, Vision: true}, Registry: r, Default: "light"})
	if err != nil || got != "light" {
		t.Fatalf("unsatisfiable turn want default light (no error), got %q err %v", got, err)
	}
}
