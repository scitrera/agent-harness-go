// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestWithAgentName_SetsNestedScitreraKey(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleAssistant)
	out := WithAgentName(msg, "Sam44")

	raw, ok := out.Meta["scitrera"]
	if !ok {
		t.Fatal("expected meta.scitrera to be set")
	}
	var nested map[string]string
	if err := json.Unmarshal(raw, &nested); err != nil {
		t.Fatalf("meta.scitrera not an object: %v", err)
	}
	if nested["agent_name"] != "Sam44" {
		t.Fatalf("agent_name = %q, want Sam44", nested["agent_name"])
	}
}

func TestWithAgentName_EmptyIsNoop(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleAssistant)
	out := WithAgentName(msg, "")
	if _, ok := out.Meta["scitrera"]; ok {
		t.Fatal("empty name should not set meta.scitrera")
	}
}

// WithAgentName must merge into an existing scitrera object, not clobber it:
// authority_grant_id written inbound must survive, and AuthorityGrantID must
// still read it.
func TestWithAgentName_MergesAndPreservesGrant(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleAssistant)
	msg.Meta = map[string]json.RawMessage{
		"scitrera":  json.RawMessage(`{"authority_grant_id":"grant-xyz"}`),
		"other_top": json.RawMessage(`"keep"`),
	}
	out := WithAgentName(msg, "Sam44")

	if got := AuthorityGrantID(out); got != "grant-xyz" {
		t.Fatalf("authority_grant_id lost: got %q", got)
	}
	var nested map[string]string
	_ = json.Unmarshal(out.Meta["scitrera"], &nested)
	if nested["agent_name"] != "Sam44" {
		t.Fatalf("agent_name not merged: %v", nested)
	}
	if string(out.Meta["other_top"]) != `"keep"` {
		t.Fatalf("other top-level meta key lost: %s", out.Meta["other_top"])
	}
	// Original message's meta must be untouched (copy-on-write).
	if _, ok := decodeScitrera(msg)["agent_name"]; ok {
		t.Fatal("WithAgentName mutated the input message's meta")
	}
}

func decodeScitrera(msg spec.ChatMessage) map[string]string {
	out := map[string]string{}
	if raw, ok := msg.Meta["scitrera"]; ok {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}
