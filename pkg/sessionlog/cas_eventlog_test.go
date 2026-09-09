// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

type memoryCASBlobStore struct {
	mu              sync.Mutex
	values          map[string][]byte
	forcedConflicts int
	alwaysConflict  bool
}

func newMemoryCASBlobStore() *memoryCASBlobStore {
	return &memoryCASBlobStore{values: map[string][]byte{}}
}

func (s *memoryCASBlobStore) Read(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	return append([]byte(nil), value...), ok, nil
}

func (s *memoryCASBlobStore) Create(_ context.Context, key string, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.values[key]; exists {
		return false, nil
	}
	s.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (s *memoryCASBlobStore) CompareAndSwap(_ context.Context, key string, expected, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.alwaysConflict {
		return false, nil
	}
	if s.forcedConflicts > 0 {
		s.forcedConflicts--
		return false, nil
	}
	current, exists := s.values[key]
	if !exists || !bytes.Equal(current, expected) {
		return false, nil
	}
	s.values[key] = append([]byte(nil), value...)
	return true, nil
}

func TestCASEventLogPersistsProjectionAcrossInstances(t *testing.T) {
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	first, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := first.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started := spec.NewChatMessage("message-1", spec.RoleAssistant)
	startedPayload, err := json.Marshal(spec.MessageStartedEvent{Message: started})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(ctx, ref, spec.SessionEventChatStream, startedPayload); err != nil {
		t.Fatal(err)
	}
	final := started.Clone()
	final.Content = []spec.ContentPart{spec.NewTextPart("distributed")}
	finalPayload, err := json.Marshal(spec.MessageFinalizedEvent{MessageID: final.ID, Message: final})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(ctx, ref, spec.SessionEventChatStream, finalPayload); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewCASEventLog(CASEventLogConfig{
		Blobs: blobs,
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
		t.Fatalf("restart messages = %#v", capture.Messages)
	}
	text, ok := capture.Messages[0].Content[0].AsText()
	if !ok || text.Text != "distributed" {
		t.Fatalf("restart projection = %#v", capture.Messages[0])
	}
}

func TestCASEventLogConcurrentReplicasAssignContiguousCursors(t *testing.T) {
	const count = 32
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	generations := sequenceGenerations()
	left, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, MaxEvents: count, MaxRetries: count * 4, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, MaxEvents: count, MaxRetries: count * 4, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	before, err := left.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	errorsCh := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			log := left
			if index%2 == 1 {
				log = right
			}
			_, err := log.Append(ctx, ref, "test", json.RawMessage(fmt.Sprintf(`{"n":%d}`, index)))
			errorsCh <- err
		}(i)
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	replay, err := right.Replay(ctx, ref, before)
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

func TestCASEventLogBoundsReplayAndIsolatesWorkspaceKeys(t *testing.T) {
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	log, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, MaxEvents: 2, NewGeneration: sequenceGenerations()})
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
	for i := 0; i < 4; i++ {
		if _, err := log.Append(ctx, projectA, "test", json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := log.Append(ctx, projectB, "test", json.RawMessage(`{"workspace":"b"}`)); err != nil {
		t.Fatal(err)
	}
	replayA, err := log.Replay(ctx, projectA, startA)
	if err != nil {
		t.Fatal(err)
	}
	replayB, err := log.Replay(ctx, projectB, startB)
	if err != nil {
		t.Fatal(err)
	}
	if replayA.Status != spec.SessionReplayPartial || len(replayA.Events) != 2 || replayA.Events[0].Cursor.Sequence != 3 {
		t.Fatalf("workspace A replay = %#v", replayA)
	}
	if replayB.Status != spec.SessionReplayComplete || len(replayB.Events) != 1 || replayB.Events[0].WorkspaceID != projectB.WorkspaceID {
		t.Fatalf("workspace B replay = %#v", replayB)
	}
	if log.key(projectA) == log.key(projectB) {
		t.Fatalf("workspace keys collided: %q", log.key(projectA))
	}
}

func TestCASEventLogRetriesConflictAndFailsClosedAtLimit(t *testing.T) {
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	log, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, MaxRetries: 2, NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	before, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	blobs.forcedConflicts = 1
	if _, err := log.Append(ctx, ref, "test", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("append after one conflict: %v", err)
	}
	blobs.alwaysConflict = true
	if _, err := log.Append(ctx, ref, "test", json.RawMessage(`{"ok":false}`)); !errors.Is(err, ErrCASRetryLimit) {
		t.Fatalf("conflict limit error = %v", err)
	}
	blobs.alwaysConflict = false
	after, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != before.Generation || after.Sequence != 1 {
		t.Fatalf("failed append changed durable cursor: before=%#v after=%#v", before, after)
	}
}

func TestCASEventLogRejectsCorruptDistributedState(t *testing.T) {
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	log, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, NewGeneration: sequenceGenerations()})
	if err != nil {
		t.Fatal(err)
	}
	blobs.values[log.key(ref)] = []byte(`{"schema_version":1,"workspace_id":"project-b"}`)
	if _, err := log.Cursor(ctx, ref); !errors.Is(err, ErrCorruptEventLog) {
		t.Fatalf("corrupt state error = %v", err)
	}
}

func TestCASEventLogResetSurvivesAnotherInstance(t *testing.T) {
	ctx := context.Background()
	blobs := newMemoryCASBlobStore()
	generations := sequenceGenerations()
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	first, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCASEventLog(CASEventLogConfig{Blobs: blobs, NewGeneration: generations})
	if err != nil {
		t.Fatal(err)
	}
	old, err := first.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(ctx, ref, "test", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	reset, err := second.Reset(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := first.Replay(ctx, ref, old)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != spec.SessionReplayUnavailable || !equalCursors(replay.Through, reset) {
		t.Fatalf("old replay after reset = %#v", replay)
	}
}
