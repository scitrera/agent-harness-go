// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package goal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestFileLedgerPersistsAppendOnlyDecisionsAndIsolatesWorkspaces(t *testing.T) {
	stateDir := t.TempDir()
	now := func() time.Time { return time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC) }
	ledger, err := NewFileLedger(stateDir, now)
	if err != nil {
		t.Fatal(err)
	}
	budget := uint64(100)
	first, err := ledger.Append(context.Background(), "project-a", "session-1", DecisionRecord{
		GoalID: "goal-1", TurnMessageID: "assistant-1",
		Action: DecisionContinuationPlanned, Reason: ReasonActiveGoal,
		TokenUsage: 25, TokenBudget: &budget, ContinuationsUsed: 1,
		Evidence: []string{"message:assistant-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Append(context.Background(), "project-a", "session-1", DecisionRecord{
		GoalID: "goal-1", TurnMessageID: "assistant-1",
		Action: DecisionContinuationEnqueued, Reason: ReasonActiveGoal,
		ContinuationsUsed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("sequences = %d, %d", first.Sequence, second.Sequence)
	}

	restarted, _ := NewFileLedger(stateDir, now)
	records, err := restarted.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != 2 || records[0].Action != DecisionContinuationPlanned {
		t.Fatalf("restarted records = %#v err=%v", records, err)
	}
	*records[0].TokenBudget = 1
	records[0].Evidence[0] = "mutated"
	again, _ := restarted.List(context.Background(), "project-a", "session-1")
	if *again[0].TokenBudget != budget || again[0].Evidence[0] != "message:assistant-1" {
		t.Fatalf("caller mutated ledger = %#v", again)
	}
	isolated, err := restarted.List(context.Background(), "project-b", "session-1")
	if err != nil || len(isolated) != 0 {
		t.Fatalf("workspace B records = %#v err=%v", isolated, err)
	}
}

func TestFileLedgerRejectsCorruption(t *testing.T) {
	ledger, _ := NewFileLedger(t.TempDir(), time.Now)
	path := ledger.path("project-a", "session-1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	corrupt := ledgerState{
		SchemaVersion: continuationLedgerSchemaVersion,
		WorkspaceID:   "project-b", SessionID: "session-1", NextSequence: 1,
		Records: []DecisionRecord{},
	}
	raw, _ := json.Marshal(corrupt)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.List(context.Background(), "project-a", "session-1"); !errors.Is(err, ErrCorruptLedger) {
		t.Fatalf("corrupt error = %v", err)
	}
}

func TestFileLedgerAdmitsOnlyFirstDecisionForTurn(t *testing.T) {
	ledger, _ := NewFileLedger(t.TempDir(), time.Now)
	decision := DecisionRecord{
		GoalID: "goal-1", TurnMessageID: "assistant-1",
		Action: DecisionContinuationPlanned, Reason: ReasonActiveGoal,
	}
	first, admitted, err := ledger.AppendFirstDecision(context.Background(), "project-a", "session-1", decision)
	if err != nil || !admitted || first.Sequence != 1 {
		t.Fatalf("first decision = %#v admitted=%v err=%v", first, admitted, err)
	}
	second, admitted, err := ledger.AppendFirstDecision(context.Background(), "project-a", "session-1", DecisionRecord{
		GoalID: "goal-1", TurnMessageID: "assistant-1",
		Action: DecisionStopped, Reason: ReasonEnqueueUnavailable,
	})
	if err != nil || admitted || second.Action != DecisionContinuationPlanned || second.Sequence != 1 {
		t.Fatalf("duplicate decision = %#v admitted=%v err=%v", second, admitted, err)
	}
	records, _ := ledger.List(context.Background(), "project-a", "session-1")
	if len(records) != 1 {
		t.Fatalf("records = %#v", records)
	}
}

func TestCASLedgerSerializesConcurrentReplicaAppends(t *testing.T) {
	blobs := newMemoryCASBlobs()
	now := func() time.Time { return time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC) }
	first, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 512, Now: now})
	second, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 512, Now: now})
	const count = 64
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ledger := first
			if i%2 == 1 {
				ledger = second
			}
			_, err := ledger.Append(context.Background(), "project-a", "session-1", DecisionRecord{
				GoalID: "goal-1", TurnMessageID: fmtID("assistant", i),
				Action: DecisionStopped, Reason: ReasonEnqueueUnavailable,
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	records, err := first.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != count {
		t.Fatalf("records=%d err=%v", len(records), err)
	}
	for i, record := range records {
		if record.Sequence != uint64(i+1) {
			t.Fatalf("record %d sequence = %d", i, record.Sequence)
		}
	}
}

func TestCASLedgerAdmitsOneFirstDecisionAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASBlobs()
	first, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 512})
	second, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 512})
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ledger := first
			if i%2 == 1 {
				ledger = second
			}
			_, admitted, err := ledger.AppendFirstDecision(context.Background(), "project-a", "session-1", DecisionRecord{
				GoalID: "goal-1", TurnMessageID: "assistant-1",
				Action: DecisionContinuationPlanned, Reason: ReasonActiveGoal,
			})
			results <- admitted
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	admitted := 0
	for result := range results {
		if result {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted decisions = %d", admitted)
	}
	records, err := first.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != 1 || records[0].Sequence != 1 {
		t.Fatalf("records = %#v err=%v", records, err)
	}
}

func fmtID(prefix string, value int) string {
	return prefix + "-" + strconv.Itoa(value)
}
