// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestTaskMessageTopic(t *testing.T) {
	if got := TaskMessageTopic("ws1", "task1"); got != "tk::ws1::task1::msg" {
		t.Fatalf("topic = %q", got)
	}
}

func TestStreamEnvelopeShape(t *testing.T) {
	meta := TurnMeta{AppWorkspace: "ws1", TaskID: "task1", ThreadID: "th1", WindowID: "win1", SourceAgent: "agent-harness:s1"}
	payload, err := StreamEnvelope(meta, spec.TokenDeltaEvent{MessageID: "m1", Index: 0, Text: "hi"})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	options, _ := env["options"].(map[string]any)
	ev, _ := options["chat_stream_event"].(map[string]any)
	if ev["event"] != "token_delta" || ev["text"] != "hi" {
		t.Fatalf("chat_stream_event wrong: %v", ev)
	}
	if options["thread_id"] != "th1" || options["app_workspace"] != "ws1" {
		t.Fatalf("options wrong: %v", options)
	}
	src, _ := env["source"].(map[string]any)
	if src["agent"] != "agent-harness:s1" {
		t.Fatalf("source.agent = %v", src["agent"])
	}
	init, _ := env["initiator"].(map[string]any)
	if init["request_id"] != "win1" || init["workspace"] != "ws1" {
		t.Fatalf("initiator wrong: %v", init)
	}
}

func TestStreamEnvelopeNullWorkspace(t *testing.T) {
	payload, err := StreamEnvelope(TurnMeta{TaskID: "t", WindowID: "w"}, spec.TokenDeltaEvent{MessageID: "m", Index: 0, Text: "x"})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := env["workspace"]; !ok || v != nil {
		t.Fatalf("workspace should be JSON null, got %v (present=%v)", v, ok)
	}
}

func TestParseInboundUserTurn(t *testing.T) {
	msg := spec.NewChatMessage("u1", spec.RoleUser)
	msg.Addr = spec.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	msg.Content = []spec.ContentPart{spec.NewTextPart("hello "), spec.NewTextPart("world")}
	msg.Meta = map[string]json.RawMessage{"scitrera": json.RawMessage(`{"authority_grant_id":"grant-9"}`)}
	raw, _ := json.Marshal(msg)

	got, ctrl, err := ParseInbound(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ctrl != nil {
		t.Fatalf("user turn should have no control: %+v", ctrl)
	}
	if ConcatText(got) != "hello world" {
		t.Fatalf("text = %q", ConcatText(got))
	}
	if AuthorityGrantID(got) != "grant-9" {
		t.Fatalf("authority = %q", AuthorityGrantID(got))
	}
}

func TestParseInboundCancel(t *testing.T) {
	msg := spec.NewChatMessage("c1", spec.RoleUser)
	msg.Content = []spec.ContentPart{spec.NewControlPart("cancel", "task-42")}
	raw, _ := json.Marshal(msg)

	_, ctrl, err := ParseInbound(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ctrl == nil || ctrl.Kind != "cancel" || ctrl.TaskID != "task-42" {
		t.Fatalf("control wrong: %+v", ctrl)
	}
}

func TestAuthorityGrantIDFallback(t *testing.T) {
	msg := spec.NewChatMessage("x", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{"authority_grant_id": json.RawMessage(`"top-level"`)}
	if got := AuthorityGrantID(msg); got != "top-level" {
		t.Fatalf("authority = %q", got)
	}
}
