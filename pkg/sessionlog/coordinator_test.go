package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestCoordinatorAttachResolvesWorkspaceAndMergesLiveProjection(t *testing.T) {
	ctx := context.Background()
	history := harness.NewMemoryStore()
	threadID := "shared-session"
	userA := testMessage("user-a", protocol.RoleUser, "project-a", threadID, "question A")
	userB := testMessage("user-b", protocol.RoleUser, "project-b", threadID, "question B")
	if err := history.SaveWorkspaceHistory(ctx, "project-a", threadID, []protocol.ChatMessage{userA}); err != nil {
		t.Fatal(err)
	}
	if err := history.SaveWorkspaceHistory(ctx, "project-b", threadID, []protocol.ChatMessage{userB}); err != nil {
		t.Fatal(err)
	}

	events := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	refA := Ref{WorkspaceID: "project-a", SessionID: threadID}
	before, err := events.Cursor(ctx, refA)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewRecordingPublisher(RecordingPublisherConfig{Events: events, DefaultWorkspace: "project-a"})
	if err != nil {
		t.Fatal(err)
	}
	assistant := testMessage("assistant-a", protocol.RoleAssistant, "", "", "answer A")
	if err := publisher.PublishEvent(ctx, channel.Event{
		Type:    channel.EventMessageFinal,
		Addr:    protocol.MessageAddress{ThreadID: threadID},
		Message: &assistant,
	}); err != nil {
		t.Fatal(err)
	}

	resolver, err := NewStaticWorkspaceResolver("project-a", []string{"project-b"})
	if err != nil {
		t.Fatal(err)
	}
	state := &messageCountState{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Workspaces: resolver,
		History:    history,
		Events:     events,
		State:      state,
		Capabilities: []spec.SessionCapability{
			spec.SessionCapabilitySnapshot,
			spec.SessionCapabilityMultiWorkspace,
			spec.SessionCapabilityReplay,
			spec.SessionCapabilityReplay,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := spec.NewSessionAttachRequest(threadID, "client-1")
	request.Capabilities = append(request.Capabilities, spec.SessionCapabilityMultiWorkspace)
	request.ResumeAfter = &before
	result, err := coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkspaceID != "project-a" || result.Snapshot.WorkspaceID != "project-a" {
		t.Fatalf("resolved workspace = %q, snapshot = %q", result.WorkspaceID, result.Snapshot.WorkspaceID)
	}
	if len(result.Snapshot.Messages) != 2 || result.Snapshot.Messages[0].ID != userA.ID || result.Snapshot.Messages[1].ID != assistant.ID {
		t.Fatalf("snapshot messages = %#v", messageIDs(result.Snapshot.Messages))
	}
	if result.Replay == nil || result.Replay.Status != spec.SessionReplayComplete || len(result.Replay.Events) != 1 {
		t.Fatalf("replay = %#v", result.Replay)
	}
	if !equalCursors(result.Replay.Through, result.Snapshot.Cursor) {
		t.Fatalf("replay through = %#v; snapshot = %#v", result.Replay.Through, result.Snapshot.Cursor)
	}
	if state.lastCount != 2 || string(result.Snapshot.State["message_count"]) != "2" {
		t.Fatalf("state count = %d, snapshot state = %s", state.lastCount, result.Snapshot.State["message_count"])
	}
	wantCaps := spec.NormalizeSessionCapabilities([]spec.SessionCapability{
		spec.SessionCapabilitySnapshot,
		spec.SessionCapabilityReplay,
		spec.SessionCapabilityMultiWorkspace,
	})
	if !equalCapabilities(result.Capabilities, wantCaps) {
		t.Fatalf("capabilities = %#v, want %#v", result.Capabilities, wantCaps)
	}

	requestB := spec.NewSessionAttachRequest(threadID, "client-2")
	requestB.WorkspaceID = "project-b"
	resultB, err := coordinator.Attach(ctx, requestB)
	if err != nil {
		t.Fatal(err)
	}
	if len(resultB.Snapshot.Messages) != 1 || resultB.Snapshot.Messages[0].ID != userB.ID {
		t.Fatalf("workspace B snapshot leaked: %#v", messageIDs(resultB.Snapshot.Messages))
	}
	if resultB.Snapshot.Cursor.Sequence != 0 {
		t.Fatalf("workspace B cursor = %#v", resultB.Snapshot.Cursor)
	}
}

func TestCoordinatorAttachUnavailableReplayAndVisibility(t *testing.T) {
	ctx := context.Background()
	history := harness.NewMemoryStore()
	events := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	resolver, err := NewStaticWorkspaceResolver("project-a", []string{"project-b"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{Workspaces: resolver, History: history, Events: events})
	if err != nil {
		t.Fatal(err)
	}

	request := spec.NewSessionAttachRequest("session-1", "client-1")
	request.ResumeAfter = &spec.SessionCursor{Generation: "retired-generation", Sequence: 7}
	result, err := coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Replay == nil || result.Replay.Status != spec.SessionReplayUnavailable {
		t.Fatalf("generation mismatch replay = %#v", result.Replay)
	}
	if !equalCursors(result.Replay.Through, result.Snapshot.Cursor) {
		t.Fatalf("unavailable replay boundary = %#v, snapshot = %#v", result.Replay.Through, result.Snapshot.Cursor)
	}

	request.WorkspaceID = "private-project"
	if _, err := coordinator.Attach(ctx, request); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("unknown workspace error = %v", err)
	}
	request.WorkspaceID = " project-a "
	if _, err := coordinator.Attach(ctx, request); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("normalized explicit workspace unexpectedly accepted: %v", err)
	}
}

func TestCoordinatorOmitsReplayWhenCapabilityIsNotNegotiated(t *testing.T) {
	ctx := context.Background()
	history := harness.NewMemoryStore()
	events := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	resolver, err := NewStaticWorkspaceResolver("project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Workspaces:   resolver,
		History:      history,
		Events:       events,
		Capabilities: []spec.SessionCapability{spec.SessionCapabilitySnapshot},
	})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := events.Cursor(ctx, Ref{WorkspaceID: "project-a", SessionID: "session-1"})
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("session-1", "client-1")
	request.ResumeAfter = &cursor
	result, err := coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Replay != nil {
		t.Fatalf("replay returned without negotiation: %#v", result.Replay)
	}
	if len(result.Capabilities) != 1 || result.Capabilities[0] != spec.SessionCapabilitySnapshot {
		t.Fatalf("capabilities = %#v", result.Capabilities)
	}
}

func TestMergeSnapshotMessagesKeepsDurableVersion(t *testing.T) {
	durable := testMessage("assistant-1", protocol.RoleAssistant, "project-a", "session-1", "persisted")
	projected := testMessage("assistant-1", protocol.RoleAssistant, "project-a", "session-1", "streamed")
	live := testMessage("assistant-2", protocol.RoleAssistant, "project-a", "session-1", "live")

	merged := mergeSnapshotMessages([]protocol.ChatMessage{durable}, []protocol.ChatMessage{projected, live})
	if len(merged) != 2 || merged[0].ID != durable.ID || merged[1].ID != live.ID {
		t.Fatalf("merged IDs = %#v", messageIDs(merged))
	}
	text, ok := merged[0].Content[0].AsText()
	if !ok || text.Text != "persisted" {
		t.Fatalf("durable message was replaced: %#v", merged[0].Content)
	}
}

type messageCountState struct {
	lastCount int
}

func (s *messageCountState) SnapshotState(_ context.Context, _, _ string, _ spec.SessionCursor, messages []protocol.ChatMessage) (map[string]json.RawMessage, error) {
	s.lastCount = len(messages)
	count, err := json.Marshal(len(messages))
	if err != nil {
		return nil, err
	}
	return map[string]json.RawMessage{"message_count": count}, nil
}

func testMessage(id string, role protocol.Role, workspaceID, threadID, text string) protocol.ChatMessage {
	message := spec.NewChatMessage(id, role)
	message.Addr.WorkspaceID = workspaceID
	message.Addr.ThreadID = threadID
	message.Content = []spec.ContentPart{spec.NewTextPart(text)}
	return message
}

func messageIDs(messages []protocol.ChatMessage) []string {
	ids := make([]string, len(messages))
	for i, message := range messages {
		ids[i] = message.ID
	}
	return ids
}

func equalCapabilities(left, right []spec.SessionCapability) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
