package tasklifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type recordingTaskOps struct {
	claimed   []string
	completed []string
	failed    []string
	claimErr  error
}

func (o *recordingTaskOps) ClaimTask(_ context.Context, taskID string) error {
	o.claimed = append(o.claimed, taskID)
	return o.claimErr
}

func (o *recordingTaskOps) CompleteTask(_ context.Context, taskID string) error {
	o.completed = append(o.completed, taskID)
	return nil
}

func (o *recordingTaskOps) FailTask(_ context.Context, taskID, _ string) error {
	o.failed = append(o.failed, taskID)
	return nil
}

type okExecutor struct{}

func (okExecutor) Run(_ context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) (protocol.ChatMessage, error) {
	return protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant}, nil
}

func TestExcludeLocalSkipsLifecycleForLocallyMintedTasks(t *testing.T) {
	// Given: a turn whose task id the harness minted for its own correlation.
	// Claiming it would fail against a host that never issued it, and a failed
	// claim fails the turn — so the whole turn must bypass the lifecycle.
	ops := &recordingTaskOps{claimErr: errors.New("task not found")}
	exec := WrapWhen(okExecutor{}, ops, ExcludeLocal)
	addr := protocol.MessageAddress{ThreadID: "t1", TaskID: "task-local"}
	message := MarkLocal(protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser})

	// When
	assistant, err := exec.Run(context.Background(), addr, message)

	// Then
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "a1" {
		t.Fatalf("assistant = %#v", assistant)
	}
	if len(ops.claimed) != 0 || len(ops.completed) != 0 {
		t.Fatalf("locally minted task went through the lifecycle: claimed=%v completed=%v", ops.claimed, ops.completed)
	}
}

func TestExcludeLocalStillManagesAuthoritativeTasks(t *testing.T) {
	// Given: an ordinary turn from the authoritative task host.
	ops := &recordingTaskOps{}
	exec := WrapWhen(okExecutor{}, ops, ExcludeLocal)
	addr := protocol.MessageAddress{ThreadID: "t1", TaskID: "task-real"}

	// When
	if _, err := exec.Run(context.Background(), addr, protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser}); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then
	if len(ops.claimed) != 1 || ops.claimed[0] != "task-real" {
		t.Fatalf("claimed = %v, want the authoritative task", ops.claimed)
	}
	if len(ops.completed) != 1 || ops.completed[0] != "task-real" {
		t.Fatalf("completed = %v, want the authoritative task", ops.completed)
	}
}

func TestUnreadableLocalMarkerIsTreatedAsAuthoritative(t *testing.T) {
	// Silently skipping a terminal transition the host is waiting on is worse
	// than a loud claim failure, so a marker we cannot parse must not exempt.
	message := protocol.ChatMessage{
		ID: "u1", Role: protocol.RoleUser,
		Meta: map[string]json.RawMessage{LocalTaskMetaKey: json.RawMessage(`"yes"`)},
	}
	if IsLocal(message) {
		t.Fatal("an unreadable marker exempted a turn from its task lifecycle")
	}
	if !ExcludeLocal(protocol.MessageAddress{}, message) {
		t.Fatal("ExcludeLocal must manage a turn whose marker is unreadable")
	}
}

func TestMarkLocalRoundTripsAndIsIdempotent(t *testing.T) {
	marked := MarkLocal(protocol.ChatMessage{ID: "u1"})
	if !IsLocal(marked) {
		t.Fatal("MarkLocal did not survive IsLocal")
	}
	if again := MarkLocal(marked); len(again.Meta) != len(marked.Meta) {
		t.Fatalf("re-marking changed meta: %#v", again.Meta)
	}
	if IsLocal(protocol.ChatMessage{ID: "u2"}) {
		t.Fatal("an unmarked message reported as locally minted")
	}
}
