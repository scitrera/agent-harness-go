// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import (
	"context"
	"reflect"
	"testing"
)

func TestBoundWorkspaceProviderMapsOnlyLogicalDefault(t *testing.T) {
	var workspaces []string
	base := WorkspaceProviderFunc(func(_ context.Context, workspaceID string) (Catalog, error) {
		workspaces = append(workspaces, workspaceID)
		return Catalog{}, nil
	})
	provider := BindWorkspaceProvider(base, "project-a", "memorylayer-a")

	for _, workspaceID := range []string{"", "project-a", "project-b"} {
		if _, err := provider.LoadWorkspace(context.Background(), workspaceID); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"memorylayer-a", "memorylayer-a", "project-b"}; !reflect.DeepEqual(workspaces, want) {
		t.Fatalf("backend workspaces = %v, want %v", workspaces, want)
	}
}

func TestMergeSkillsFilesystemOverrideWinsWithoutReordering(t *testing.T) {
	remote := []SkillSpec{
		{Name: "alpha", Content: "remote alpha", Enabled: true, AllowedTools: []string{"remote"}},
		{Name: "zeta", Content: "remote zeta", Enabled: true},
	}
	local := []SkillSpec{
		{Name: "alpha", Content: "local alpha", Enabled: true, AllowedTools: []string{"local"}},
		{Name: "beta", Content: "local beta", Enabled: true},
		{Name: "disabled", Content: "hidden", Enabled: false},
	}

	got := MergeSkills(remote, local)
	if len(got) != 3 || got[0].Name != "alpha" || got[1].Name != "zeta" || got[2].Name != "beta" {
		t.Fatalf("merged order = %#v", got)
	}
	if got[0].Content != "local alpha" || !reflect.DeepEqual(got[0].AllowedTools, []string{"local"}) {
		t.Fatalf("filesystem override = %#v", got[0])
	}
	local[0].AllowedTools[0] = "mutated"
	if got[0].AllowedTools[0] != "local" {
		t.Fatal("merged catalog retained caller-owned slice")
	}
}

func TestComputedRevisionIgnoresPriorRevisionAndTracksContent(t *testing.T) {
	first, err := WithComputedRevision(Catalog{Revision: "stale", Skills: []SkillSpec{{Name: "alpha", Content: "one", Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := WithComputedRevision(Catalog{Revision: "different", Skills: []SkillSpec{{Name: "alpha", Content: "one", Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := WithComputedRevision(Catalog{Skills: []SkillSpec{{Name: "alpha", Content: "two", Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision == "" || first.Revision != second.Revision || first.Revision == changed.Revision {
		t.Fatalf("revisions: first=%q second=%q changed=%q", first.Revision, second.Revision, changed.Revision)
	}
	ctx := WithRevision(context.Background(), first.Revision)
	if got, ok := RevisionFrom(ctx); !ok || got != first.Revision {
		t.Fatalf("bound revision = %q, %v", got, ok)
	}
}

func TestComputedRevisionTracksAuthoritativeResourceRevision(t *testing.T) {
	first, err := WithComputedRevision(Catalog{Skills: []SkillSpec{{
		Name: "alpha", Content: "same", Enabled: true,
		Source: &ResourceRevision{System: "memorylayer", ID: "skill-a", Revision: 1, ETag: "etag-1", ManifestDigest: "manifest"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Revision = ""
	second.Skills = append([]SkillSpec(nil), first.Skills...)
	source := *second.Skills[0].Source
	source.ETag = "etag-2"
	second.Skills[0].Source = &source
	second, err = WithComputedRevision(second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision == second.Revision {
		t.Fatalf("catalog revision did not rotate on authoritative ETag change: %q", first.Revision)
	}
}
