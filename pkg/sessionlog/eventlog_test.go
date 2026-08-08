package sessionlog

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestMemoryEventLogReplayCoverageAndIsolation(t *testing.T) {
	log := NewMemoryEventLog(MemoryEventLogConfig{MaxEvents: 2, NewGeneration: sequenceGenerations()})
	ctx := context.Background()
	projectA := Ref{WorkspaceID: "project-a", SessionID: "shared"}
	projectB := Ref{WorkspaceID: "project-b", SessionID: "shared"}

	startA, err := log.Cursor(ctx, projectA)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if _, err := log.Append(ctx, projectA, "test", json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}

	partial, err := log.Replay(ctx, projectA, startA)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Status != spec.SessionReplayPartial || len(partial.Events) != 2 {
		t.Fatalf("partial replay = %#v", partial)
	}
	if partial.Events[0].Cursor.Sequence != 3 || partial.Events[1].Cursor.Sequence != 4 {
		t.Fatalf("retained sequences = %d, %d", partial.Events[0].Cursor.Sequence, partial.Events[1].Cursor.Sequence)
	}

	complete, err := log.Replay(ctx, projectA, spec.SessionCursor{Generation: startA.Generation, Sequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	if complete.Status != spec.SessionReplayComplete || len(complete.Events) != 2 {
		t.Fatalf("complete replay = %#v", complete)
	}

	cursorB, err := log.Cursor(ctx, projectB)
	if err != nil {
		t.Fatal(err)
	}
	if cursorB.Sequence != 0 || cursorB.Generation == startA.Generation {
		t.Fatalf("workspace B cursor = %#v; workspace A start = %#v", cursorB, startA)
	}
	replayB, err := log.Replay(ctx, projectB, cursorB)
	if err != nil {
		t.Fatal(err)
	}
	if replayB.Status != spec.SessionReplayComplete || len(replayB.Events) != 0 {
		t.Fatalf("workspace B replay = %#v", replayB)
	}
}

func TestMemoryEventLogResetMakesOldCursorUnavailable(t *testing.T) {
	log := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}

	old, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, ref, "test", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	reset, err := log.Reset(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Generation == old.Generation || reset.Sequence != 0 {
		t.Fatalf("reset cursor = %#v; old = %#v", reset, old)
	}
	replay, err := log.Replay(ctx, ref, old)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != spec.SessionReplayUnavailable || len(replay.Events) != 0 || !equalCursors(replay.Through, reset) {
		t.Fatalf("old-generation replay = %#v", replay)
	}
}

func TestMemoryEventLogClonesPayloadAndRejectsCrossWorkspaceStream(t *testing.T) {
	log := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	payload := json.RawMessage(`{"value":"original"}`)
	event, err := log.Append(ctx, ref, "test", payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[10] = 'X'
	event.Payload[10] = 'Y'
	cursor := spec.SessionCursor{Generation: event.Cursor.Generation}
	replay, err := log.Replay(ctx, ref, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(replay.Events[0].Payload); got != `{"value":"original"}` {
		t.Fatalf("stored payload mutated: %s", got)
	}

	message := spec.NewChatMessage("message-1", spec.RoleAssistant)
	message.Addr.WorkspaceID = "project-b"
	message.Addr.ThreadID = ref.SessionID
	streamPayload, err := json.Marshal(spec.MessageStartedEvent{Message: message})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, ref, spec.SessionEventChatStream, streamPayload); err == nil {
		t.Fatal("expected cross-workspace stream event to be rejected")
	}
}

func TestMemoryEventLogConcurrentAppendIsContiguous(t *testing.T) {
	const count = 64
	log := NewMemoryEventLog(MemoryEventLogConfig{MaxEvents: count, NewGeneration: sequenceGenerations()})
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	before, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	errors := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(value int) {
			defer wait.Done()
			_, err := log.Append(ctx, ref, "test", json.RawMessage(fmt.Sprintf(`{"n":%d}`, value)))
			errors <- err
		}(i)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}

	replay, err := log.Replay(ctx, ref, before)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != spec.SessionReplayComplete || len(replay.Events) != count {
		t.Fatalf("replay = %#v", replay)
	}
	for i, event := range replay.Events {
		if event.Cursor.Sequence != uint64(i+1) {
			t.Fatalf("event %d sequence = %d", i, event.Cursor.Sequence)
		}
	}
}

func equalCursors(left, right spec.SessionCursor) bool {
	return left.Generation == right.Generation && left.Sequence == right.Sequence
}

func sequenceGenerations() func() (string, error) {
	var mu sync.Mutex
	var next int
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		next++
		return fmt.Sprintf("generation-%d", next), nil
	}
}
