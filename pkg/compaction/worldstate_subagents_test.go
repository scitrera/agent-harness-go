// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package compaction

import "testing"

func TestActiveSubagentHandles_TTLAndOrder(t *testing.T) {
	ws := WorldState{Turn: 10, ActiveSubagents: []SubagentHandle{
		{ID: "t::old", Name: "old", LastTurn: 1},       // age 9
		{ID: "t::recent", Name: "recent", LastTurn: 9}, // age 1
		{ID: "t::now", Name: "now", LastTurn: 10},      // age 0
	}}
	// TTL 5 drops "old" (age 9); result ordered most-recent-first.
	active := ws.ActiveSubagentHandles(5)
	if len(active) != 2 {
		t.Fatalf("active = %v, want 2 (old dropped)", active)
	}
	if active[0].Name != "now" || active[1].Name != "recent" {
		t.Fatalf("order = %v, want [now recent]", active)
	}
	// TTL 0 = no limit.
	if len(ws.ActiveSubagentHandles(0)) != 3 {
		t.Fatal("ttl 0 should keep all")
	}
}

func TestMergeWorldState_SubagentResumeKeepsMostRecent(t *testing.T) {
	a := WorldState{Turn: 3, ActiveSubagents: []SubagentHandle{
		{ID: "t::sub::1", Name: "reviewer", Status: "completed", Summary: "first pass", LastTurn: 1},
	}}
	// The same sub-agent (same thread_id) resumed at a later turn: it should
	// UPDATE the entry (keep max LastTurn), not duplicate it.
	b := WorldState{Turn: 5, ActiveSubagents: []SubagentHandle{
		{ID: "t::sub::1", Name: "reviewer", Status: "completed", Summary: "second pass", LastTurn: 5},
	}}
	got := MergeWorldState(a, b)
	if len(got.ActiveSubagents) != 1 {
		t.Fatalf("ActiveSubagents = %v, want 1 (deduped by thread_id)", got.ActiveSubagents)
	}
	h := got.ActiveSubagents[0]
	if h.LastTurn != 5 || h.Summary != "second pass" {
		t.Fatalf("kept handle = %+v, want the resume (LastTurn 5, second pass)", h)
	}
}
