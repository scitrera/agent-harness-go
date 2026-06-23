// Package hooks defines in-process extension points for the turn loop. Today it
// covers tool gating (approve/deny) and tool lifecycle observation; the
// interfaces are deliberately transport-agnostic so an external implementation
// (e.g. an Aether-ACL-driven approver, or an OTel observer) can satisfy the same
// contracts later without changing the turn loop.
package hooks

import (
	"context"
	"encoding/json"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// ToolCall is the subset of a tool invocation a hook inspects.
type ToolCall struct {
	CallID string
	Name   string
	Args   json.RawMessage
	Addr   protocol.MessageAddress
}

// Decision is an approver's verdict on a tool call.
type Decision struct {
	Allow  bool
	Reason string // human-readable; surfaced to the model when denied
}

// Allow returns an allowing decision.
func Allow() Decision { return Decision{Allow: true} }

// Deny returns a denying decision with a reason.
func Deny(reason string) Decision { return Decision{Allow: false, Reason: reason} }

// ToolApprover decides whether a tool call may execute. The first denial wins.
// External approvers (e.g. Aether-ACL-driven) implement this same interface.
type ToolApprover interface {
	ApproveTool(ctx context.Context, call ToolCall) Decision
}

// ToolObserver observes the tool lifecycle without a veto. Audit/telemetry
// (e.g. OTel spans) implement this.
type ToolObserver interface {
	ToolStarted(ctx context.Context, call ToolCall)
	ToolFinished(ctx context.Context, call ToolCall, isError bool, err error)
}

// Approve runs approvers in order and returns the first denial, or Allow when
// none object. nil/empty approver slices allow everything.
func Approve(ctx context.Context, call ToolCall, approvers []ToolApprover) Decision {
	for _, a := range approvers {
		if a == nil {
			continue
		}
		if d := a.ApproveTool(ctx, call); !d.Allow {
			return d
		}
	}
	return Allow()
}

// AllowList is a ToolApprover permitting only the named tools. An empty list
// permits everything (it is treated as "no restriction"), which is how an
// absent command allowed-tools frontmatter behaves.
type AllowList struct {
	names  map[string]bool
	reason string
}

// NewAllowList builds an AllowList from tool names. The reason is surfaced to
// the model on denial.
func NewAllowList(names []string, reason string) *AllowList {
	if len(names) == 0 {
		return &AllowList{}
	}
	set := make(map[string]bool, len(names))
	for _, n := range names {
		if n != "" {
			set[n] = true
		}
	}
	return &AllowList{names: set, reason: reason}
}

// ApproveTool allows the call when the list is empty (no restriction) or the
// tool is listed.
func (a *AllowList) ApproveTool(_ context.Context, call ToolCall) Decision {
	if a == nil || len(a.names) == 0 {
		return Allow()
	}
	if a.names[call.Name] {
		return Allow()
	}
	reason := a.reason
	if reason == "" {
		reason = "tool not permitted for this turn"
	}
	return Deny(reason)
}
