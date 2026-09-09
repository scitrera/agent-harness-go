// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package protocol

import (
	"encoding/json"
	"testing"
)

func TestToolCallPartRoundTrip(t *testing.T) {
	env := ToolInvokeEnvelope{
		CallID: "call-1",
		Name:   "edit_file",
		Args:   map[string]json.RawMessage{"path": json.RawMessage(`"SOUL.md"`)},
	}
	part, err := NewToolCallPart(env)
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	if part.Type() != ContentToolCall {
		t.Fatalf("type = %s", part.Type())
	}
	got, ok := ToolCallFromPart(part)
	if !ok {
		t.Fatal("expected tool call envelope")
	}
	if got.CallID != "call-1" || got.Name != "edit_file" {
		t.Fatalf("envelope = %+v", got)
	}
	if string(got.Args["path"]) != `"SOUL.md"` {
		t.Fatalf("args = %v", got.Args)
	}
}

func TestArgsRawRoundTrip(t *testing.T) {
	args := RawToArgs(json.RawMessage(`{"path":"SOUL.md","n":1}`))
	if string(args["path"]) != `"SOUL.md"` {
		t.Fatalf("path = %s", args["path"])
	}
	var got map[string]any
	if err := json.Unmarshal(ArgsToRaw(args), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["path"] != "SOUL.md" {
		t.Fatalf("round-trip path = %v", got["path"])
	}
}

func TestNewToolResultPartUsesOutput(t *testing.T) {
	part, err := NewToolResultPart("call-1", "edit_file", json.RawMessage(`{"ok":true}`), false)
	if err != nil {
		t.Fatalf("tool result part: %v", err)
	}
	tr, ok := part.AsToolResult()
	if !ok {
		t.Fatal("expected tool result")
	}
	if tr.CallID != "call-1" {
		t.Fatalf("call_id = %s", tr.CallID)
	}
	if string(tr.Output) != `{"ok":true}` {
		t.Fatalf("output = %s", tr.Output)
	}
}

func TestTextPartRoundTrip(t *testing.T) {
	part, err := NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	tp, ok := part.AsText()
	if !ok || tp.Text != "hello" {
		t.Fatalf("text = %q ok=%v", tp.Text, ok)
	}
}

func TestSubagentPartRoundTrip(t *testing.T) {
	part, err := NewSubagentPart(SubagentPart{
		ID:       "call-1",
		Name:     "reviewer",
		ThreadID: "parent::sub::3",
		Status:   SubagentCompleted,
		Summary:  "did the thing",
	})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}
	if part.Type() != ContentSubagent {
		t.Fatalf("type = %s", part.Type())
	}
	sp, ok := part.AsSubagent()
	if !ok {
		t.Fatal("expected subagent part")
	}
	if sp.ID != "call-1" || sp.Name != "reviewer" || sp.ThreadID != "parent::sub::3" {
		t.Fatalf("fields not preserved: %+v", sp)
	}
	if sp.Status != SubagentCompleted || sp.Summary != "did the thing" {
		t.Fatalf("status/summary not preserved: %+v", sp)
	}
}

func TestSubagentPartDefaultsStatusPending(t *testing.T) {
	part, err := NewSubagentPart(SubagentPart{ID: "c1", ThreadID: "t"})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}
	sp, ok := part.AsSubagent()
	if !ok {
		t.Fatal("expected subagent part")
	}
	if sp.Status != SubagentPending {
		t.Fatalf("status = %q, want pending", sp.Status)
	}
}

func TestUnknownPartTypePreserved(t *testing.T) {
	var msg ChatMessage
	raw := []byte(`{"id":"m1","role":"user","addr":{"thread_id":"thr"},"content":[{"type":"image","uri":"vfs://image","x-extra":{"kept":true}}]}`)
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(msg.Content) != 1 || msg.Content[0].Type() != ContentImage {
		t.Fatalf("image part not preserved: %#v", msg.Content)
	}
}
