// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"testing"
)

type denyTool struct{ name string }

func (d denyTool) ApproveTool(_ context.Context, call ToolCall) Decision {
	if call.Name == d.name {
		return Deny("blocked: " + d.name)
	}
	return Allow()
}

func TestAllowList(t *testing.T) {
	al := NewAllowList([]string{"read_file", "list_dir"}, "not permitted")
	if d := al.ApproveTool(context.Background(), ToolCall{Name: "read_file"}); !d.Allow {
		t.Fatal("read_file should be allowed")
	}
	if d := al.ApproveTool(context.Background(), ToolCall{Name: "shell"}); d.Allow {
		t.Fatal("shell should be denied")
	} else if d.Reason != "not permitted" {
		t.Fatalf("unexpected reason %q", d.Reason)
	}

	// Empty list = no restriction.
	empty := NewAllowList(nil, "x")
	if d := empty.ApproveTool(context.Background(), ToolCall{Name: "anything"}); !d.Allow {
		t.Fatal("empty allow-list should permit everything")
	}
}

func TestApproveFirstDenialWins(t *testing.T) {
	approvers := []ToolApprover{denyTool{name: "shell"}, denyTool{name: "write_file"}}
	if d := Approve(context.Background(), ToolCall{Name: "read_file"}, approvers); !d.Allow {
		t.Fatal("read_file should pass all approvers")
	}
	if d := Approve(context.Background(), ToolCall{Name: "write_file"}, approvers); d.Allow {
		t.Fatal("write_file should be denied by second approver")
	}
	// nil entries are skipped; empty slice allows.
	if d := Approve(context.Background(), ToolCall{Name: "x"}, []ToolApprover{nil}); !d.Allow {
		t.Fatal("nil approver should be skipped")
	}
}
