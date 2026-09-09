// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// partTag renders a content part as "<kind>:<id-or-text>" so a test can assert
// on the reconstructed content order (== stream order) of a finalized turn.
func partTag(p protocol.ContentPart) string {
	if tp, ok := p.AsText(); ok {
		return "text:" + tp.Text
	}
	if env, ok := protocol.ToolCallFromPart(p); ok {
		return "tool_call:" + env.CallID
	}
	if ar, ok := p.AsApprovalRequest(); ok {
		return "approval:" + ar.ID
	}
	return string(p.Type()) // tool_result, reasoning, image, …
}

func contentSeq(msg protocol.ChatMessage) []string {
	out := make([]string, 0, len(msg.Content))
	for _, p := range msg.Content {
		out = append(out, partTag(p))
	}
	return out
}

func indexOfTag(seq []string, tag string) int {
	for i, s := range seq {
		if s == tag {
			return i
		}
	}
	return -1
}

// Issue (2): a NON-streaming provider returns an assistant message whose text
// preamble precedes its tool_call. That preamble must render inline at its real
// position (before the call), not be dropped so the turn's only text collapses
// to the finalize tail.
func Test_Runner_Run_nonstreaming_preamble_text_renders_before_tool_call(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	preamble, err := protocol.NewTextPart("Let me record that.")
	if err != nil {
		t.Fatalf("preamble text: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("All set.")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	// scriptedProvider implements Chat only (non-streaming): no token deltas, so
	// the preamble reaches the stream only if the tool loop surfaces it.
	prov := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{preamble, callPart}}},
		{Message: protocol.ChatMessage{ID: "a-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          prov,
		Publisher:         &fakePublisher{},
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
		// Streaming deliberately OFF: exercises the path where nothing is streamed
		// live, so the preamble would be lost without the tool-loop surfacing it.
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "th1"}, userTurn(t))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	seq := contentSeq(assistant)
	want := []string{"text:Let me record that.", "tool_call:call-1", "tool_result", "text:All set."}
	if len(seq) != len(want) {
		t.Fatalf("content order = %v, want %v", seq, want)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("content order = %v, want %v", seq, want)
		}
	}
}

// Issue (1): with multiple tool calls that each require approval, the
// approval_request for a call must render adjacent to (right after) ITS
// tool_call — not after the whole batch of tool calls.
func Test_Runner_Run_approval_renders_adjacent_to_its_tool_call(t *testing.T) {
	ran := 0
	policy := tools.NewDynamicPolicy(tools.StaticPolicy{Allowed: map[string]string{}}, nil)
	registry := tools.NewAuditedRegistry(policy, nil)
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		ran++
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	call1, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("call1: %v", err)
	}
	call2, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-2", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("call2: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final: %v", err)
	}
	prov := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a-tools", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{call1, call2}}},
		{Message: protocol.ChatMessage{ID: "a-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          prov,
		Publisher:         &fakePublisher{},
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
		// "once" scope: each call re-prompts (no session/always grant recorded), so
		// both call-1 and call-2 emit their own approval_request.
		Approvals:       fakeAwaiter{decision: approval.Decision{Granted: true, Scope: "once"}},
		ApprovalGranter: policy,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	assistant, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "th1", TaskID: "t1", WorkspaceID: "ws1"}, userTurn(t))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if ran != 2 {
		t.Fatalf("both approved tools should run, ran=%d", ran)
	}
	seq := contentSeq(assistant)
	tc1, ap1 := indexOfTag(seq, "tool_call:call-1"), indexOfTag(seq, "approval:call-1")
	tc2, ap2 := indexOfTag(seq, "tool_call:call-2"), indexOfTag(seq, "approval:call-2")
	if tc1 < 0 || ap1 < 0 || tc2 < 0 || ap2 < 0 {
		t.Fatalf("missing expected parts in %v", seq)
	}
	// Each approval sits right after its own call …
	if tc1 >= ap1 || tc2 >= ap2 {
		t.Fatalf("approval must follow its own tool_call: %v", seq)
	}
	// … and call-1's approval must precede call-2 (the regression: approvals were
	// emitted after the whole tool_call batch, so ap1 landed after tc2).
	if ap1 >= tc2 {
		t.Fatalf("call-1 approval must render before call-2's tool_call, got order %v", seq)
	}
}
