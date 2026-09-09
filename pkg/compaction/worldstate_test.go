// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package compaction

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func Test_ReduceWithReport_preserves_world_state_and_context_budget_when_history_compacted(t *testing.T) {
	// Given
	state := WorldState{
		PlanRefs: []PlanRef{{ID: "plan-1", Path: ".omo/plans/claude-code-adoption.md", Status: "approved"}},
		Tasks:    []TaskState{{ID: "todo-9", Title: "compaction recovery", Status: "in_progress", BlockedBy: []string{"todo-1"}}},
		Todos:    []TodoState{{ID: "check-tests", Content: "run targeted tests", Status: "pending"}},
		InvokedSkills: []SkillRef{
			{Name: "omo:programming", Source: "go"},
		},
		RecentFiles:     []FileMetadata{{Path: "oss/pkg/compaction/compact.go", Digest: "sha256:abc", Bytes: 123}},
		DynamicTools:    []string{"mcp.calc.add", "todo_write"},
		ActiveSubagents: []SubagentHandle{{ID: "agent-1", Task: "inspect recovery", Status: "running"}},
	}
	messages := []protocol.ChatMessage{
		testMessage(t, "m1", protocol.RoleUser, "oldest"),
		testMessage(t, "m2", protocol.RoleAssistant, "middle"),
		testMessage(t, "m3", protocol.RoleUser, "recent"),
		testMessage(t, "m4", protocol.RoleAssistant, "newest"),
	}

	// When
	report, err := ReduceWithReport(messages, Config{MaxMessages: 2, MaxTokens: 10000, WorldState: state})

	// Then
	if err != nil {
		t.Fatalf("reduce with report: %v", err)
	}
	if len(report.Messages) != 3 || report.Messages[0].ID != "compaction-summary" {
		t.Fatalf("expected summary plus two recent messages, got %#v", ids(report.Messages))
	}
	if report.Budget.MaxTokens != 10000 || report.Budget.EstimatedTokens <= 0 || report.Budget.RemainingTokens <= 0 {
		t.Fatalf("expected populated budget, got %#v", report.Budget)
	}
	var got WorldState
	if err := json.Unmarshal(report.Messages[0].Meta[MetaWorldState], &got); err != nil {
		t.Fatalf("decode world state meta: %v", err)
	}
	if !reflect.DeepEqual(got.PlanRefs, state.PlanRefs) ||
		!reflect.DeepEqual(got.Tasks, state.Tasks) ||
		!reflect.DeepEqual(got.Todos, state.Todos) ||
		!reflect.DeepEqual(got.InvokedSkills, state.InvokedSkills) ||
		!reflect.DeepEqual(got.RecentFiles, state.RecentFiles) ||
		!reflect.DeepEqual(got.DynamicTools, state.DynamicTools) ||
		!reflect.DeepEqual(got.ActiveSubagents, state.ActiveSubagents) {
		t.Fatalf("world state was not preserved: %#v", got)
	}
	var budget ContextBudget
	if err := json.Unmarshal(report.Messages[0].Meta[MetaContextBudget], &budget); err != nil {
		t.Fatalf("decode budget meta: %v", err)
	}
	if budget.MessageCount != len(report.Messages) || budget.CompactedMessages != 2 {
		t.Fatalf("unexpected budget meta: %#v", budget)
	}
}

func Test_LocalTranscriptStore_Recover_resumes_compacted_thread_with_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	store := NewLocalTranscriptStore(filepath.Join(t.TempDir(), "thread.jsonl"))
	state := WorldState{
		PlanRefs:      []PlanRef{{ID: "plan-1", Path: "plan.md", Status: "approved"}},
		Tasks:         []TaskState{{ID: "task-1", Title: "recover", Status: "completed"}},
		InvokedSkills: []SkillRef{{Name: "omo:programming"}},
	}
	report, err := ReduceWithReport([]protocol.ChatMessage{
		testMessage(t, "u1", protocol.RoleUser, "first"),
		testMessage(t, "a1", protocol.RoleAssistant, "answer"),
		testMessage(t, "u2", protocol.RoleUser, "latest"),
	}, Config{MaxMessages: 2, MaxTokens: 10000, WorldState: state})
	if err != nil {
		t.Fatalf("reduce with report: %v", err)
	}
	if err := store.AppendSegment(ctx, TranscriptSegment{
		ID:         "segment-1",
		Compacted:  true,
		Messages:   report.Messages,
		WorldState: report.WorldState,
		Budget:     report.Budget,
	}); err != nil {
		t.Fatalf("append segment: %v", err)
	}
	localHistory := []protocol.ChatMessage{testMessage(t, "u2", protocol.RoleUser, "latest")}

	// When
	recovered, err := store.Recover(ctx, localHistory)

	// Then
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %#v", recovered.Warnings)
	}
	if recovered.Source != RecoverySourceTranscript || !recovered.Compacted {
		t.Fatalf("expected transcript recovery, got source=%q compacted=%v", recovered.Source, recovered.Compacted)
	}
	if countMessageID(recovered.Messages, "u2") != 1 {
		t.Fatalf("expected no duplicate user turn, got messages %#v", ids(recovered.Messages))
	}
	if recovered.WorldState.PlanRefs[0].ID != "plan-1" || recovered.WorldState.InvokedSkills[0].Name != "omo:programming" {
		t.Fatalf("expected world state from transcript, got %#v", recovered.WorldState)
	}
	if recovered.Budget.RemainingTokens <= 0 {
		t.Fatalf("expected remaining context budget, got %#v", recovered.Budget)
	}
}

func Test_LocalTranscriptStore_Recover_reads_large_transcript_segment(t *testing.T) {
	// Given
	ctx := context.Background()
	store := NewLocalTranscriptStore(filepath.Join(t.TempDir(), "thread.jsonl"))
	largeText := strings.Repeat("x", 96*1024)
	largeMessage := testMessage(t, "large", protocol.RoleAssistant, largeText)
	if err := store.AppendSegment(ctx, TranscriptSegment{
		ID:       "segment-large",
		Messages: []protocol.ChatMessage{largeMessage},
	}); err != nil {
		t.Fatalf("append segment: %v", err)
	}

	// When
	recovered, err := store.Recover(ctx, nil)

	// Then
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered.Source != RecoverySourceTranscript {
		t.Fatalf("expected transcript recovery, got %q", recovered.Source)
	}
	if len(recovered.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %#v", recovered.Warnings)
	}
	if len(recovered.Messages) != 1 || recovered.Messages[0].ID != "large" {
		t.Fatalf("expected large transcript message, got %#v", ids(recovered.Messages))
	}
}

func Test_LocalTranscriptStore_Recover_degrades_to_local_history_when_segment_corrupt(t *testing.T) {
	// Given
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "thread.jsonl")
	store := NewLocalTranscriptStore(path)
	if err := store.AppendSegment(ctx, TranscriptSegment{
		ID:       "valid-before-corrupt",
		Messages: []protocol.ChatMessage{testMessage(t, "u1", protocol.RoleUser, "local question")},
	}); err != nil {
		t.Fatalf("append valid segment: %v", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open transcript: %v", err)
	}
	if _, err := f.WriteString("{not-json}\n"); err != nil {
		t.Fatalf("write corrupt segment: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close transcript: %v", err)
	}
	localHistory := []protocol.ChatMessage{testMessage(t, "u1", protocol.RoleUser, "local question")}

	// When
	recovered, err := store.Recover(ctx, localHistory)

	// Then
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered.Source != RecoverySourceLocal {
		t.Fatalf("expected local fallback, got %q", recovered.Source)
	}
	if len(recovered.Warnings) != 1 || recovered.Warnings[0].Kind != RecoveryWarningCorruptSegment {
		t.Fatalf("expected corrupt warning, got %#v", recovered.Warnings)
	}
	if len(recovered.Messages) != 1 || countMessageID(recovered.Messages, "u1") != 1 {
		t.Fatalf("expected exactly local history without duplicate, got %#v", ids(recovered.Messages))
	}
}

func testMessage(t *testing.T, id string, role protocol.Role, text string) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return protocol.ChatMessage{ID: id, Role: role, Content: []protocol.ContentPart{part}}
}

func countMessageID(messages []protocol.ChatMessage, id string) int {
	count := 0
	for _, msg := range messages {
		if msg.ID == id {
			count++
		}
	}
	return count
}
