// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestFileEventLogPersistsInitialCursorAndProjectionAcrossRestart(t *testing.T) {
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	stateDir := t.TempDir()
	generations := sequenceGenerations()
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	started := spec.NewChatMessage("message-1", spec.RoleAssistant)
	startedPayload, err := json.Marshal(spec.MessageStartedEvent{Message: started})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, ref, spec.SessionEventChatStream, startedPayload); err != nil {
		t.Fatal(err)
	}
	final := started.Clone()
	final.Content = []spec.ContentPart{spec.NewTextPart("persisted")}
	finalPayload, err := json.Marshal(spec.MessageFinalizedEvent{MessageID: final.ID, Message: final})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, ref, spec.SessionEventChatStream, finalPayload); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewFileEventLog(FileEventLogConfig{
		StateDir: stateDir,
		NewGeneration: func() (string, error) {
			return "", errors.New("persisted session unexpectedly requested a generation")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	capture, err := restarted.Capture(ctx, ref, &initial)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Cursor.Generation != initial.Generation || capture.Cursor.Sequence != 2 {
		t.Fatalf("restart cursor = %#v; initial = %#v", capture.Cursor, initial)
	}
	if capture.Replay == nil || capture.Replay.Status != spec.SessionReplayComplete || len(capture.Replay.Events) != 2 {
		t.Fatalf("restart replay = %#v", capture.Replay)
	}
	if len(capture.Messages) != 1 {
		t.Fatalf("restart projection messages = %d", len(capture.Messages))
	}
	text, ok := capture.Messages[0].Content[0].AsText()
	if !ok || text.Text != "persisted" {
		t.Fatalf("restart projection content = %#v", capture.Messages[0].Content)
	}
	if capture.Messages[0].Addr.WorkspaceID != ref.WorkspaceID || capture.Messages[0].Addr.ThreadID != ref.SessionID {
		t.Fatalf("restart projection address = %#v", capture.Messages[0].Addr)
	}
}

func TestFileEventLogRetainsBoundedSuffixAndWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	generations := sequenceGenerations()
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, MaxEvents: 2, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	projectA := Ref{WorkspaceID: "project-a", SessionID: "shared"}
	projectB := Ref{WorkspaceID: "project-b", SessionID: "shared"}
	startA, err := log.Cursor(ctx, projectA)
	if err != nil {
		t.Fatal(err)
	}
	startB, err := log.Cursor(ctx, projectB)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if _, err := log.Append(ctx, projectA, "test", json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := log.Append(ctx, projectB, "test", json.RawMessage(`{"workspace":"b"}`)); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, MaxEvents: 2, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	replayA, err := restarted.Replay(ctx, projectA, startA)
	if err != nil {
		t.Fatal(err)
	}
	if replayA.Status != spec.SessionReplayPartial || len(replayA.Events) != 2 || replayA.Events[0].Cursor.Sequence != 3 || replayA.Events[1].Cursor.Sequence != 4 {
		t.Fatalf("workspace A replay = %#v", replayA)
	}
	replayB, err := restarted.Replay(ctx, projectB, startB)
	if err != nil {
		t.Fatal(err)
	}
	if replayB.Status != spec.SessionReplayComplete || len(replayB.Events) != 1 || replayB.Events[0].WorkspaceID != projectB.WorkspaceID {
		t.Fatalf("workspace B replay = %#v", replayB)
	}
	if replayA.Through.Generation == replayB.Through.Generation {
		t.Fatalf("workspace generations collided: %#v %#v", replayA.Through, replayB.Through)
	}
	if log.eventPath(projectA) == log.eventPath(projectB) {
		t.Fatalf("workspace paths collided: %s", log.eventPath(projectA))
	}
}

func TestFileEventLogResetSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	stateDir := t.TempDir()
	generations := sequenceGenerations()
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
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

	restarted, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.Replay(ctx, ref, old)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != spec.SessionReplayUnavailable || len(replay.Events) != 0 || !equalCursors(replay.Through, reset) {
		t.Fatalf("old-generation replay after restart = %#v", replay)
	}
	capture, err := restarted.Capture(ctx, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.Messages) != 0 || !equalCursors(capture.Cursor, reset) {
		t.Fatalf("reset capture after restart = %#v", capture)
	}
}

func TestFileEventLogRejectsCorruptState(t *testing.T) {
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	stateDir := t.TempDir()
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Cursor(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(log.eventPath(ref), []byte(`{"schema_version":1,"workspace_id":"project-b"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Cursor(ctx, ref); !errors.Is(err, ErrCorruptEventLog) {
		t.Fatalf("corrupt state error = %v", err)
	}
}

func TestFileEventLogConcurrentAppendPersistsContiguousReplay(t *testing.T) {
	const count = 64
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	stateDir := t.TempDir()
	generations := sequenceGenerations()
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, MaxEvents: count, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
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

	restarted, err := NewFileEventLog(FileEventLogConfig{StateDir: stateDir, MaxEvents: count, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.Replay(ctx, ref, before)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != spec.SessionReplayComplete || len(replay.Events) != count {
		t.Fatalf("restart replay = %#v", replay)
	}
	for i, event := range replay.Events {
		if event.Cursor.Sequence != uint64(i+1) {
			t.Fatalf("event %d sequence = %d", i, event.Cursor.Sequence)
		}
	}
}

func TestFileEventLogRollsBackMemoryWhenPersistenceFails(t *testing.T) {
	ctx := context.Background()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	log, err := NewFileEventLog(FileEventLogConfig{StateDir: t.TempDir(), NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	before, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	path := log.eventPath(ref)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, ref, "test", json.RawMessage(`{"ok":true}`)); err == nil {
		t.Fatal("append unexpectedly succeeded with an unwritable event path")
	}
	after, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !equalCursors(after, before) {
		t.Fatalf("failed append advanced cursor: before=%#v after=%#v", before, after)
	}
}
