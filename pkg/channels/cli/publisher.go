// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package cli is a reference egress implementation of the transport seam: a
// channel.Publisher that renders a turn's stream events to a writer (e.g.
// stdout). Pair it with a stdin REPL that calls turn.Runner.Run directly — the
// idiomatic CLI shape — rather than the poll-based runtime loop.
package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// Publisher renders stream events to w. It prints streamed token deltas inline;
// if a turn produces no deltas (non-streaming provider), it prints the finalized
// message text instead, so output is correct in both modes.
type Publisher struct {
	w           io.Writer
	sawDelta    bool
	sawToolCall bool
}

// NewPublisher returns a Publisher writing to w.
func NewPublisher(w io.Writer) *Publisher { return &Publisher{w: w} }

// PublishEvent implements channel.Publisher.
func (p *Publisher) PublishEvent(_ context.Context, e channel.Event) error {
	switch e.Type {
	case channel.EventMessageStarted:
		p.sawDelta = false
		p.sawToolCall = false
	case channel.EventTokenDelta:
		if e.Delta != "" {
			p.sawDelta = true
			_, _ = fmt.Fprint(p.w, e.Delta)
		}
	case channel.EventPartAppended:
		if e.Part != nil {
			if tc, ok := e.Part.AsToolCall(); ok {
				p.sawToolCall = true
				_, _ = fmt.Fprintf(p.w, "\n  · %s(%s)\n", tc.Name, string(protocol.ArgsToRaw(tc.Args)))
			}
		}
	case channel.EventMessageFinal:
		if !p.sawDelta && e.Message != nil {
			_, _ = fmt.Fprint(p.w, messageText(*e.Message))
		}
		_, _ = fmt.Fprintln(p.w)
	}
	return nil
}

var _ channel.Publisher = (*Publisher)(nil)

func messageText(m protocol.ChatMessage) string {
	out := ""
	for _, part := range m.Content {
		if tp, ok := part.AsText(); ok {
			out += tp.Text
		}
	}
	return out
}
