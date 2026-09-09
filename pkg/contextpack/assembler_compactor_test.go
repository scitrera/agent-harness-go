// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package contextpack

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// recordingCompactor is a Compactor stub that records it was called and returns
// the history unchanged.
type recordingCompactor struct{ called bool }

func (c *recordingCompactor) Compact(_ context.Context, messages []protocol.ChatMessage, cfg compaction.Config) (compaction.Report, error) {
	c.called = true
	return compaction.ReduceWithReport(messages, cfg)
}

// A non-nil Config.Compactor is used in place of the built-in ReduceWithReport.
func Test_Assembler_Build_uses_injected_compactor(t *testing.T) {
	p, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	history := []protocol.ChatMessage{{ID: "m1", Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}}

	rec := &recordingCompactor{}
	a := NewAssembler(Config{Compactor: rec})
	if _, err := a.Build(context.Background(), nil, history); err != nil {
		t.Fatalf("build: %v", err)
	}
	if !rec.called {
		t.Fatalf("expected the injected Compactor to be invoked")
	}
}
