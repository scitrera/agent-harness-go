package compaction

import "testing"

func TestMergeWorldState_TurnMaxAndSkillMostRecent(t *testing.T) {
	a := WorldState{Turn: 3, InvokedSkills: []SkillRef{{Name: "x", LastTurn: 1}, {Name: "y", LastTurn: 3}}}
	b := WorldState{Turn: 5, InvokedSkills: []SkillRef{{Name: "x", LastTurn: 4}}} // x re-invoked later
	got := MergeWorldState(a, b)
	if got.Turn != 5 {
		t.Fatalf("Turn = %d, want 5 (max)", got.Turn)
	}
	byName := map[string]SkillRef{}
	for _, s := range got.InvokedSkills {
		byName[s.Name] = s
	}
	if byName["x"].LastTurn != 4 {
		t.Fatalf("x LastTurn = %d, want 4 (most recent)", byName["x"].LastTurn)
	}
	if byName["y"].LastTurn != 3 {
		t.Fatalf("y LastTurn = %d, want 3", byName["y"].LastTurn)
	}
}

func TestActiveInvokedSkills_TTLAndOrder(t *testing.T) {
	ws := WorldState{Turn: 10, InvokedSkills: []SkillRef{
		{Name: "old", LastTurn: 1},    // age 9
		{Name: "recent", LastTurn: 9}, // age 1
		{Name: "now", LastTurn: 10},   // age 0
	}}
	// TTL 5 drops "old" (age 9); result ordered most-recent-first.
	active := ws.ActiveInvokedSkills(5)
	if len(active) != 2 {
		t.Fatalf("active = %v, want 2 (old dropped)", active)
	}
	if active[0].Name != "now" || active[1].Name != "recent" {
		t.Fatalf("order = %v, want [now recent]", active)
	}
	if ws.SkillAge(ws.InvokedSkills[0]) != 9 {
		t.Fatalf("age(old) = %d, want 9", ws.SkillAge(ws.InvokedSkills[0]))
	}
	// TTL 0 = no limit.
	if len(ws.ActiveInvokedSkills(0)) != 3 {
		t.Fatal("ttl 0 should keep all")
	}
}
