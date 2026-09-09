// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func msg(t *testing.T, text string) *protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	m := protocol.ChatMessage{Role: protocol.RoleAssistant, Content: []protocol.ContentPart{p}}
	return &m
}

func TestPublisherStreamsDeltas(t *testing.T) {
	var b bytes.Buffer
	p := NewPublisher(&b)
	ctx := context.Background()
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventMessageStarted})
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventTokenDelta, Delta: "hel"})
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventTokenDelta, Delta: "lo"})
	// Final carries the full message; since deltas streamed, it must NOT be re-printed.
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventMessageFinal, Message: msg(t, "hello")})
	if got := b.String(); !strings.HasPrefix(got, "hello") || strings.Count(got, "hello") != 1 {
		t.Fatalf("expected streamed 'hello' once, got %q", got)
	}
}

func TestPublisherFinalWithoutDeltas(t *testing.T) {
	var b bytes.Buffer
	p := NewPublisher(&b)
	ctx := context.Background()
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventMessageStarted})
	// No deltas (non-streaming provider) -> final message text is printed.
	_ = p.PublishEvent(ctx, channel.Event{Type: channel.EventMessageFinal, Message: msg(t, "full answer")})
	if !strings.Contains(b.String(), "full answer") {
		t.Fatalf("expected final text printed, got %q", b.String())
	}
}
