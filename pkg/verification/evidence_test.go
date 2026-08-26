package verification

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestObserverReportsConfiguredEvidenceWithoutPayloads(t *testing.T) {
	fixed := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	observer, err := NewObserver(Config{Now: func() time.Time { return fixed }, Recipes: []Recipe{
		{Name: "package tests", ToolName: "shell", ArgumentsContain: []string{"go test"}, Scope: CheckTargeted},
		{Name: "full tests", ToolName: "shell", ArgumentsContain: []string{"go test ./..."}, Scope: CheckBroader},
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "session", TaskID: "task"}
	ctx := tools.WithWorkingDirectory(context.Background(), "/workspace/project")
	observer.ToolResult(ctx, hooks.ToolCall{
		CallID: "write-1", Name: "write_file", Args: json.RawMessage(`{"path":"secret.go","content":"do not retain"}`), Addr: addr,
	}, tools.Result{Metadata: tools.ResultMetadata{FileChanges: []tools.FileChange{{Path: "secret.go", Kind: "modified"}}}}, nil)
	observer.ToolResult(ctx, hooks.ToolCall{
		CallID: "check-1", Name: "shell", Args: json.RawMessage(`{"command":"go test ./..."}`), Addr: addr,
	}, tools.Result{Payload: json.RawMessage(`{"stdout":"large output must not be retained"}`)}, nil)

	assessment := observer.Assess(addr)
	if assessment.Status != StatusBroaderPassed || !assessment.MaterialWrites || len(assessment.ChangedPaths) != 1 || assessment.ChangedPaths[0] != "secret.go" {
		t.Fatalf("assessment = %#v", assessment)
	}
	records := observer.Records(addr)
	if len(records) != 2 || records[0].WorkingDirectory != "/workspace/project" || records[1].CheckName != "full tests" {
		t.Fatalf("records = %#v", records)
	}
	encoded, _ := json.Marshal(records)
	if string(encoded) == "" || contains(string(encoded), "do not retain") || contains(string(encoded), "large output") {
		t.Fatalf("records retained arguments or output: %s", encoded)
	}
	if _, ok := observer.ConsumeFinishNudge(addr); ok {
		t.Fatal("successful broader check should suppress finish nudge")
	}
}

func TestObserverNudgesOnceWhenWritesLackChecks(t *testing.T) {
	observer, err := NewObserver(Config{})
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "session"}
	observer.ToolResult(context.Background(), hooks.ToolCall{CallID: "write", Name: "write_file", Addr: addr}, tools.Result{
		Metadata: tools.ResultMetadata{FileChanges: []tools.FileChange{{Path: "a.go", Kind: "created"}}},
	}, nil)
	if text, ok := observer.ConsumeFinishNudge(addr); !ok || text == "" {
		t.Fatal("expected first finish nudge")
	}
	if _, ok := observer.ConsumeFinishNudge(addr); ok {
		t.Fatal("finish nudge must be bounded to one")
	}
}

func contains(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
