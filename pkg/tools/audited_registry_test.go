package tools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_Registry_Invoke_records_audit_and_runs_handler_when_policy_allows(t *testing.T) {
	// Given
	ctx := context.Background()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := NewFileAuditSink(auditPath)
	if err != nil {
		t.Fatalf("audit sink: %v", err)
	}
	reg := NewAuditedRegistry(StaticPolicy{Allowed: map[string]string{"edit_file": "local file edits are allowed"}}, audit)
	called := false
	if err := reg.Register("edit_file", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		called = true
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	result, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "edit_file", Arguments: json.RawMessage(`{"path":"SOUL.md"}`)})

	// Then
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !called || result.CallID != "c1" {
		t.Fatalf("unexpected result/called: %#v %v", result, called)
	}
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if !strings.Contains(string(data), `"decision":"allow"`) || !strings.Contains(string(data), `"tool_name":"edit_file"`) {
		t.Fatalf("unexpected audit log: %s", string(data))
	}
}

func Test_Registry_Invoke_records_audit_and_blocks_handler_when_policy_requires_approval(t *testing.T) {
	// Given
	ctx := context.Background()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := NewFileAuditSink(auditPath)
	if err != nil {
		t.Fatalf("audit sink: %v", err)
	}
	reg := NewAuditedRegistry(StaticPolicy{Allowed: map[string]string{"edit_file": "allowed"}}, audit)
	called := false
	if err := reg.Register("shell", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		called = true
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	_, err = reg.Invoke(ctx, Request{CallID: "c2", Name: "shell", Arguments: json.RawMessage(`{"command":"sh"}`)})

	// Then
	if !errors.Is(err, ErrToolRequiresApproval) {
		t.Fatalf("expected ErrToolRequiresApproval, got %v", err)
	}
	if called {
		t.Fatalf("handler was called despite approval requirement")
	}
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if !strings.Contains(string(data), `"decision":"requires_approval"`) || !strings.Contains(string(data), `"tool_name":"shell"`) {
		t.Fatalf("unexpected audit log: %s", string(data))
	}
}
