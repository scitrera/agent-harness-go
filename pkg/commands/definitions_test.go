package commands

import "testing"

func TestDefinitionsDriveReservedNamesAndAliases(t *testing.T) {
	seen := map[string]bool{}
	for _, definition := range Definitions(SurfaceRunner) {
		key := CanonicalKey(definition.Name)
		if seen[key] {
			t.Fatalf("duplicate runner definition %q", key)
		}
		seen[key] = true
		if description, ok := ReservedNames[key]; !ok || description != definition.Description {
			t.Fatalf("reserved metadata for %q = %q, %v", key, description, ok)
		}
		canonical, ok := CanonicalBuiltin(definition.Name)
		if !ok {
			t.Fatalf("definition %q is not resolvable", definition.Name)
		}
		want := definition.Name
		if definition.AliasFor != "" {
			want = definition.AliasFor
		}
		if canonical != want {
			t.Fatalf("canonical %q = %q, want %q", definition.Name, canonical, want)
		}
	}
	if len(seen) != len(ReservedNames) {
		t.Fatalf("definitions=%d reserved=%d", len(seen), len(ReservedNames))
	}
}

func TestDefinitionsReturnDetachedSlices(t *testing.T) {
	first := Definitions(SurfaceTUI)
	if len(first) == 0 {
		t.Fatal("no TUI definitions")
	}
	first[0].Name = "mutated"
	if got := Definitions(SurfaceTUI)[0].Name; got == "mutated" {
		t.Fatal("definitions retained caller mutation")
	}
}
