// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	"encoding/json"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestSynthesizeSystemPrompt_ReadsSystemAndInstructions(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{
		"scitrera": json.RawMessage(`{"ephemeral":true,"synthesize_options":{"system":"be terse","instructions":"return JSON","timeout_s":30}}`),
	}
	got := SynthesizeSystemPrompt(msg)
	if !strings.Contains(got, "be terse") || !strings.Contains(got, "return JSON") {
		t.Fatalf("expected system+instructions, got %q", got)
	}
	// system precedes instructions.
	if strings.Index(got, "be terse") > strings.Index(got, "return JSON") {
		t.Errorf("system should precede instructions: %q", got)
	}
}

func TestSynthesizeSystemPrompt_AbsentIsEmpty(t *testing.T) {
	if SynthesizeSystemPrompt(spec.NewChatMessage("m1", spec.RoleUser)) != "" {
		t.Error("no meta should yield empty")
	}
	msg := spec.NewChatMessage("m2", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{"scitrera": json.RawMessage(`{"ephemeral":true}`)}
	if SynthesizeSystemPrompt(msg) != "" {
		t.Error("no synthesize_options should yield empty")
	}
}
