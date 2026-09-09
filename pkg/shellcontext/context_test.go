// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package shellcontext_test

import (
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
)

func TestRecordRoundTripAndRendering(t *testing.T) {
	record := shellcontext.Record{Command: "pwd", CWD: "/work", Output: "/work\n", ExitCode: 0, TriggerAgent: false}
	message := protocol.ChatMessage{}
	if err := shellcontext.Put(&message, record); err != nil {
		t.Fatal(err)
	}
	got, ok := shellcontext.FromMessage(message)
	if !ok || got != record {
		t.Fatalf("round trip = %#v, %v", got, ok)
	}
	if !shellcontext.IsContextOnly(message) {
		t.Fatal("expected context-only message")
	}
	if text := shellcontext.ContextText(record); !strings.Contains(text, "[shell_context]") || !strings.Contains(text, `"command": "pwd"`) {
		t.Fatalf("context text = %q", text)
	}
	if text := shellcontext.DisplayText(record); text != "$ pwd\n/work\n[exit 0]" {
		t.Fatalf("display text = %q", text)
	}
}

func TestCommitAck(t *testing.T) {
	message := protocol.ChatMessage{}
	shellcontext.MarkCommitAck(&message)
	if !shellcontext.IsCommitAck(message) {
		t.Fatal("expected commit acknowledgement")
	}
}
