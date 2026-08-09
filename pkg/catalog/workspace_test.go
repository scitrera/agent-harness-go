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
