// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type fakeRemoteInvoker struct {
	called bool
	env    protocol.ToolInvokeEnvelope
}

func (f *fakeRemoteInvoker) InvokeTool(_ context.Context, env protocol.ToolInvokeEnvelope) (Result, error) {
	f.called = true
	f.env = env
	return NewJSONResult(env.CallID, env.Name, json.RawMessage(`{"ok":true}`))
}

type memoryAuditSink struct {
	records []AuditRecord
}

func (s *memoryAuditSink) Record(_ context.Context, record AuditRecord) error {
	s.records = append(s.records, record)
	return nil
}

func Test_RegisterRemote_invokes_aether_when_policy_allows_tool(t *testing.T) {
	// Given
	ctx := context.Background()
	invoker := &fakeRemoteInvoker{}
	audit := &memoryAuditSink{}
	reg := NewRegistry()
	policy := StaticPolicy{Allowed: map[string]string{"remote_calc": "safe test tool"}}
	if err := RegisterRemote(reg, []string{"remote_calc"}, RemoteConfig{Invoker: invoker, Policy: policy, Audit: audit}); err != nil {
		t.Fatalf("register remote: %v", err)
	}

	// When
	result, err := reg.Invoke(ctx, Request{CallID: "r1", Name: "remote_calc", Arguments: json.RawMessage(`{"x":1}`)})

	// Then
	if err != nil {
		t.Fatalf("invoke remote: %v", err)
	}
	if !invoker.called || invoker.env.Name != "remote_calc" || result.CallID != "r1" {
		t.Fatalf("unexpected invocation: called=%v env=%#v result=%#v", invoker.called, invoker.env, result)
	}
	if len(audit.records) != 1 || audit.records[0].Decision != DecisionAllow || audit.records[0].ArgumentsHash == "" {
		t.Fatalf("unexpected audit records: %#v", audit.records)
	}
}

func Test_RegisterRemote_requires_approval_when_tool_is_not_allowed(t *testing.T) {
	// Given
	ctx := context.Background()
	invoker := &fakeRemoteInvoker{}
	audit := &memoryAuditSink{}
	reg := NewRegistry()
	if err := RegisterRemote(reg, []string{"remote_calc"}, RemoteConfig{Invoker: invoker, Policy: StaticPolicy{}, Audit: audit}); err != nil {
		t.Fatalf("register remote: %v", err)
	}

	// When
	_, err := reg.Invoke(ctx, Request{CallID: "r1", Name: "remote_calc", Arguments: json.RawMessage(`{"x":1}`)})

	// Then
	if !errors.Is(err, ErrToolRequiresApproval) {
		t.Fatalf("expected approval error, got %v", err)
	}
	if invoker.called {
		t.Fatalf("remote invoker should not be called")
	}
	if len(audit.records) != 1 || audit.records[0].Decision != DecisionRequiresApproval {
		t.Fatalf("unexpected audit records: %#v", audit.records)
	}
}
