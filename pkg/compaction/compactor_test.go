package compaction

import (
	"context"
	"reflect"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// fakeSink is an in-memory EvictionSink that records every offloaded blob keyed
// by its ref, so tests can assert the full content was stored.
type fakeSink struct {
	stored map[string][]byte
}

func newFakeSink() *fakeSink { return &fakeSink{stored: map[string][]byte{}} }

func (s *fakeSink) Put(_ context.Context, key string, content []byte) (string, error) {
	ref := "sink://" + key
	s.stored[ref] = append([]byte(nil), content...)
	return ref, nil
}

func toolResultMsg(t *testing.T, id, callID, output string) protocol.ChatMessage {
	t.Helper()
	// OutputText carries the human-readable (multi-line) tool output that eviction
	// previews line-by-line.
	p := spec.NewToolResultPart(spec.ToolResultPartBody{CallID: callID, Name: "some_tool", OutputText: output})
	return protocol.ChatMessage{ID: id, Role: protocol.RoleTool, Content: []protocol.ContentPart{p}}
}

func TestEvictingCompactorEvictsOldToolResult(t *testing.T) {
	// A big multi-line tool-result in an old message, plus a small recent one.
	bigOutput := strings.Repeat("line of output\n", 500)
	old := toolResultMsg(t, "old", "call-1", bigOutput)
	recentBig := toolResultMsg(t, "recent", "call-2", bigOutput)
	// Fill the recent window so recentBig sits inside it and is preserved.
	history := []protocol.ChatMessage{
		old,
		msg(t, "r1", "a"), msg(t, "r2", "b"), msg(t, "r3", "c"),
		msg(t, "r4", "d"), msg(t, "r5", "e"), recentBig,
	}

	sink := newFakeSink()
	rep, err := EvictingCompactor{Sink: sink, KeepRecent: 6}.Compact(context.Background(), history, Config{})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}

	// The old tool-result is now a preview text part with a ref back to the sink.
	tp, ok := rep.Messages[0].Content[0].AsText()
	if !ok {
		t.Fatalf("expected old tool-result replaced by a text preview, got %s", rep.Messages[0].Content[0].Type())
	}
	if !strings.Contains(tp.Text, "sink://call-1") || !strings.Contains(tp.Text, "evicted") {
		t.Fatalf("preview missing ref/marker: %q", tp.Text)
	}
	if len(tp.Text) >= len(bigOutput) {
		t.Fatalf("preview should be smaller than the evicted content (%d vs %d)", len(tp.Text), len(bigOutput))
	}

	// The full content is recorded in the sink under the call id.
	stored, ok := sink.stored["sink://call-1"]
	if !ok {
		t.Fatalf("expected evicted content stored under call id, have %v", keysOf(sink.stored))
	}
	if !strings.Contains(string(stored), "line of output") {
		t.Fatalf("stored content unexpected: %q", string(stored)[:40])
	}

	// The recent-window tool-result is untouched (never evicted).
	if _, ok := rep.Messages[len(rep.Messages)-1].Content[0].AsToolResult(); !ok {
		t.Fatalf("recent-window tool-result must be preserved, not evicted")
	}
	if _, ok := sink.stored["sink://call-2"]; ok {
		t.Fatalf("recent-window content must not be offloaded")
	}
}

func TestEvictingCompactorNilSinkDegradesToClassic(t *testing.T) {
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{msg(t, "m1", body), msg(t, "m2", body), msg(t, "m3", body)}
	cfg := Config{MaxTokens: 1200}

	got, err := EvictingCompactor{}.Compact(context.Background(), in, cfg)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	want, err := ReduceWithReport(in, cfg)
	if err != nil {
		t.Fatalf("classic: %v", err)
	}
	if !reflect.DeepEqual(ids(got.Messages), ids(want.Messages)) {
		t.Fatalf("nil-sink eviction should match classic: got %v want %v", ids(got.Messages), ids(want.Messages))
	}
}

func TestClassicCompactorMatchesReduceWithReport(t *testing.T) {
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{msg(t, "m1", body), msg(t, "m2", body), msg(t, "m3", body), msg(t, "m4", body)}
	cfg := Config{MaxMessages: 3, MaxTokens: 2500}

	got, err := ClassicCompactor{}.Compact(context.Background(), in, cfg)
	if err != nil {
		t.Fatalf("classic compactor: %v", err)
	}
	want, err := ReduceWithReport(in, cfg)
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ClassicCompactor must equal ReduceWithReport:\n got=%#v\nwant=%#v", got.Budget, want.Budget)
	}
}

// stubPrimary returns messages unchanged with a budget over the cap, forcing the
// composite to fall through to its fallback.
type stubPrimary struct{}

func (stubPrimary) Compact(_ context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error) {
	out := cloneMessages(messages)
	return Report{Messages: out, Budget: BudgetFor(out, cfg.MaxTokens, 0)}, nil
}

func TestCompositeFallsThroughWhenOverBudget(t *testing.T) {
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{msg(t, "m1", body), msg(t, "m2", body), msg(t, "m3", body)}
	cfg := Config{MaxTokens: 1200}

	comp := CompositeCompactor{Primary: stubPrimary{}, Fallback: ClassicCompactor{}}
	rep, err := comp.Compact(context.Background(), in, cfg)
	if err != nil {
		t.Fatalf("composite: %v", err)
	}
	// stubPrimary kept all 3 (over budget) -> fallback drop-oldest keeps only newest.
	if got := ids(rep.Messages); len(got) != 1 || got[0] != "m3" {
		t.Fatalf("expected fallback to trim to newest, got %v", got)
	}
	if rep.Budget.EstimatedTokens > cfg.MaxTokens {
		t.Fatalf("fallback result still over budget: %#v", rep.Budget)
	}
}

func TestCompositeKeepsPrimaryWhenUnderBudget(t *testing.T) {
	in := []protocol.ChatMessage{msg(t, "a", "short"), msg(t, "b", "also short")}
	cfg := Config{MaxTokens: 100000}
	comp := CompositeCompactor{Primary: stubPrimary{}, Fallback: ClassicCompactor{}}
	rep, err := comp.Compact(context.Background(), in, cfg)
	if err != nil {
		t.Fatalf("composite: %v", err)
	}
	if got := ids(rep.Messages); len(got) != 2 {
		t.Fatalf("under budget should keep primary output, got %v", got)
	}
}

func TestSummarizingCompactorNilSummarizerDegradesToClassic(t *testing.T) {
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{msg(t, "m1", body), msg(t, "m2", body), msg(t, "m3", body)}
	cfg := Config{MaxTokens: 1200}

	got, err := SummarizingCompactor{}.Compact(context.Background(), in, cfg)
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	want, err := ReduceWithReport(in, cfg)
	if err != nil {
		t.Fatalf("classic: %v", err)
	}
	if !reflect.DeepEqual(ids(got.Messages), ids(want.Messages)) {
		t.Fatalf("nil-summarizer should match classic: got %v want %v", ids(got.Messages), ids(want.Messages))
	}
}

type fixedSummarizer struct{ text string }

func (s fixedSummarizer) Summarize(_ context.Context, _ []protocol.ChatMessage) (string, error) {
	return s.text, nil
}

func TestSummarizingCompactorFoldsOlderMessages(t *testing.T) {
	body := strings.Repeat("x", 4000)
	in := make([]protocol.ChatMessage, 0, 10)
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"} {
		in = append(in, msg(t, id, body))
	}
	comp := SummarizingCompactor{Summarizer: fixedSummarizer{text: "SUMMARY"}, KeepRecent: 4}
	rep, err := comp.Compact(context.Background(), in, Config{MaxTokens: 2000})
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	// 1 summary head + 4 recent.
	if got := ids(rep.Messages); len(got) != 5 || got[0] != compactionSummaryID || got[4] != "m10" {
		t.Fatalf("expected summary head + recent window, got %v", got)
	}
	if tp, ok := rep.Messages[0].Content[0].AsText(); !ok || !strings.Contains(tp.Text, "SUMMARY") {
		t.Fatalf("summary head missing summarizer text")
	}
	if rep.Budget.CompactedMessages != 6 {
		t.Fatalf("expected 6 messages folded, got %d", rep.Budget.CompactedMessages)
	}
}

func TestCompactorForSelectsMode(t *testing.T) {
	sink := newFakeSink()
	if _, ok := CompactorFor("classic", sink, nil).(ClassicCompactor); !ok {
		t.Fatalf("classic mode should yield ClassicCompactor")
	}
	for _, mode := range []string{"", "evict", "composite", "bogus"} {
		if _, ok := CompactorFor(mode, sink, nil).(CompositeCompactor); !ok {
			t.Fatalf("mode %q should yield a CompositeCompactor", mode)
		}
	}
	sum, ok := CompactorFor("summarize", sink, fixedSummarizer{}).(CompositeCompactor)
	if !ok {
		t.Fatalf("summarize mode should yield a CompositeCompactor")
	}
	if _, ok := sum.Primary.(SummarizingCompactor); !ok {
		t.Fatalf("summarize mode primary should be SummarizingCompactor")
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
