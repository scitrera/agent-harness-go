// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package contextpack

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

type fakeRelevance struct {
	res SkillRelevanceResult
	err error
	got SkillRelevanceRequest
}

func (f *fakeRelevance) RankSkills(_ context.Context, req SkillRelevanceRequest) (SkillRelevanceResult, error) {
	f.got = req
	return f.res, f.err
}

func catalog() []sysprompt.SkillSummary {
	return []sysprompt.SkillSummary{
		{Name: "alpha", Description: "does alpha"},
		{Name: "beta", Description: "does beta"},
		{Name: "gamma", Description: "does gamma"},
	}
}

func promptText(t *testing.T, msgs []protocol.ChatMessage) string {
	t.Helper()
	tp, ok := msgs[0].Content[0].AsText()
	if !ok {
		t.Fatalf("system prompt not text: %#v", msgs[0])
	}
	return tp.Text
}

// No provider: the full catalog stays in the (cacheable) prefix, nothing dynamic.
func Test_Relevance_nil_provider_lists_full_catalog(t *testing.T) {
	a := NewAssembler(Config{Skills: catalog()})
	msgs, err := a.Build(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	txt := promptText(t, msgs)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if !strings.Contains(txt, name) {
			t.Fatalf("catalog skill %q missing:\n%s", name, txt)
		}
	}
	if strings.Contains(txt, "Auto-loaded skills") {
		t.Fatalf("did not expect auto-loaded section without a provider:\n%s", txt)
	}
}

// Replace mode: only the ranked skills are listed, in order, with notes.
func Test_Relevance_replace_lists_only_ranked(t *testing.T) {
	prov := &fakeRelevance{res: SkillRelevanceResult{
		Mode:   ListReplace,
		Listed: []RankedSkill{{Name: "gamma", Note: "top match"}, {Name: "alpha"}},
	}}
	a := NewAssembler(Config{Skills: catalog(), SkillRelevance: prov})
	txt := promptText(t, mustBuild(t, a))
	if !strings.Contains(txt, "gamma") || !strings.Contains(txt, "top match") || !strings.Contains(txt, "alpha") {
		t.Fatalf("expected ranked gamma/alpha with note:\n%s", txt)
	}
	if strings.Contains(txt, "beta") {
		t.Fatalf("replace mode should hide unranked beta:\n%s", txt)
	}
	// gamma should precede alpha (ranked order).
	if strings.Index(txt, "gamma") > strings.Index(txt, "alpha") {
		t.Fatalf("ranked order not preserved:\n%s", txt)
	}
}

// Refine mode: ranked first, then the remaining catalog skills.
func Test_Relevance_refine_keeps_unranked_after(t *testing.T) {
	prov := &fakeRelevance{res: SkillRelevanceResult{
		Mode:   ListRefine,
		Listed: []RankedSkill{{Name: "gamma"}},
	}}
	a := NewAssembler(Config{Skills: catalog(), SkillRelevance: prov})
	txt := promptText(t, mustBuild(t, a))
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if !strings.Contains(txt, name) {
			t.Fatalf("refine should keep %q:\n%s", name, txt)
		}
	}
	if i := strings.Index(txt, "gamma"); i > strings.Index(txt, "alpha") || i > strings.Index(txt, "beta") {
		t.Fatalf("refine should surface gamma first:\n%s", txt)
	}
}

// Auto-realize injects a body; dedups against a fresh load_skill and honors the
// byte cap.
func Test_Relevance_autorealize_injects_and_dedups(t *testing.T) {
	bodies := map[string]string{
		"alpha": "ALPHA BODY",
		"beta":  "BETA BODY",
		"gamma": strings.Repeat("G", 10_000),
	}
	prov := &fakeRelevance{res: SkillRelevanceResult{
		Realize: []string{"alpha", "beta", "gamma"},
	}}
	a := NewAssembler(Config{
		Skills:              catalog(),
		SkillRelevance:      prov,
		SkillBodies:         func(n string) (string, bool) { b, ok := bodies[n]; return b, ok },
		MaxAutoRealizeBytes: 100, // fits alpha+beta, not gamma
	})
	// beta was loaded via load_skill last turn (age 1) -> deduped (still in context).
	// Seed WorldState so workingState reports beta as realized at age 1.
	ctx := compaction.WithTurnNumber(context.Background(), 5)
	history := worldStateHistory(t, compaction.WorldState{
		Turn:          4,
		InvokedSkills: []compaction.SkillRef{{Name: "beta", LastTurn: 4}},
	})
	msgs, err := a.Build(ctx, nil, history)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	txt := promptText(t, msgs)
	if !strings.Contains(txt, "## Auto-loaded skills") || !strings.Contains(txt, "ALPHA BODY") {
		t.Fatalf("expected alpha auto-loaded:\n%s", txt)
	}
	if strings.Contains(txt, "BETA BODY") {
		t.Fatalf("beta was freshly load_skill'd; should be deduped, not re-injected:\n%s", txt)
	}
	if strings.Contains(txt, "GGGG") {
		t.Fatalf("gamma exceeds the byte cap; should be skipped:\n%s", txt)
	}
	// The provider saw beta as realized.
	if len(prov.got.Realized) == 0 || prov.got.Realized[0].Name != "beta" {
		t.Fatalf("provider should receive realized beta: %#v", prov.got.Realized)
	}
}

// A provider error falls back to the full catalog, unchanged.
func Test_Relevance_provider_error_falls_back(t *testing.T) {
	prov := &fakeRelevance{err: errors.New("boom")}
	a := NewAssembler(Config{Skills: catalog(), SkillRelevance: prov})
	txt := promptText(t, mustBuild(t, a))
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if !strings.Contains(txt, name) {
			t.Fatalf("fallback should list full catalog (%q):\n%s", name, txt)
		}
	}
}

func mustBuild(t *testing.T, a Assembler) []protocol.ChatMessage {
	t.Helper()
	msgs, err := a.Build(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return msgs
}

// worldStateHistory returns a single message carrying the given WorldState in meta,
// as ExtractWorldState reads it.
func worldStateHistory(t *testing.T, ws compaction.WorldState) []protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart("prior")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	raw, err := json.Marshal(ws)
	if err != nil {
		t.Fatalf("marshal world state: %v", err)
	}
	msg := protocol.ChatMessage{
		ID:      "prior",
		Role:    protocol.RoleAssistant,
		Content: []protocol.ContentPart{part},
		Meta:    map[string]json.RawMessage{compaction.MetaWorldState: raw},
	}
	return []protocol.ChatMessage{msg}
}
