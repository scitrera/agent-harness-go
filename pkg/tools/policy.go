package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

type DecisionCode string

const (
	DecisionAllow            DecisionCode = "allow"
	DecisionRequiresApproval DecisionCode = "requires_approval"
	DecisionDeny             DecisionCode = "deny"
)

type Decision struct {
	Code      DecisionCode `json:"code"`
	Reason    string       `json:"reason"`
	AuditCode string       `json:"audit_code"`
}

func (d Decision) Allowed() bool {
	return d.Code == DecisionAllow
}

type Policy interface {
	Decide(req Request) Decision
}

type StaticPolicy struct {
	Allowed map[string]string
}

func (p StaticPolicy) Decide(req Request) Decision {
	if p.Allowed == nil {
		return Decision{Code: DecisionRequiresApproval, Reason: "tool is not pre-authorized", AuditCode: "tool.approval_required"}
	}
	if reason, ok := p.Allowed[req.Name]; ok {
		return Decision{Code: DecisionAllow, Reason: reason, AuditCode: "tool.allow"}
	}
	return Decision{Code: DecisionRequiresApproval, Reason: "tool is not pre-authorized", AuditCode: "tool.approval_required"}
}

// EffectLookup resolves a tool name to its declared portable effect. The bool
// is false for a tool that declared none — an unclassified tool must never be
// treated as read-only by omission.
type EffectLookup func(tool string) (spec.ToolEffect, bool)

// ReadOnlyAutoApprove is the read-only approval tier: it allows any tool whose
// declared effect is `read` and defers everything else to the base policy.
//
// It exists because an always-ask configuration that prompts for `read_file`
// teaches users to approve without reading the prompt, which costs more safety
// than the prompt ever bought. Only an EXPLICIT read effect qualifies: a tool
// with no declared effect, or one whose effect the provider could not classify,
// falls through to the base policy unchanged.
type ReadOnlyAutoApprove struct {
	Base   Policy
	Effect EffectLookup
}

// NewReadOnlyAutoApprove wraps base so read-effect tools resolved through lookup
// are auto-approved. A nil lookup disables the tier (every call defers to base),
// so a misconfiguration fails closed rather than approving everything.
func NewReadOnlyAutoApprove(base Policy, lookup EffectLookup) ReadOnlyAutoApprove {
	return ReadOnlyAutoApprove{Base: base, Effect: lookup}
}

func (p ReadOnlyAutoApprove) Decide(req Request) Decision {
	if p.Effect != nil {
		if effect, ok := p.Effect(req.Name); ok && effect == spec.ToolEffectRead {
			return Decision{Code: DecisionAllow, Reason: "read-only tool", AuditCode: "tool.read_only"}
		}
	}
	if p.Base == nil {
		return Decision{Code: DecisionRequiresApproval, Reason: "tool is not pre-authorized", AuditCode: "tool.approval_required"}
	}
	return p.Base.Decide(req)
}

// GrantStore persists "always"-scoped tool authorizations per workspace. The
// approval flow writes to it on an "always" grant; DynamicPolicy hydrates from
// it. Implemented by the distribution (e.g. a MemoryLayer-backed store). Phase-3
// surface; DynamicPolicy works without one (session-only).
type GrantStore interface {
	// ListGranted returns the tools durably authorized for a workspace.
	ListGranted(ctx context.Context, workspaceID string) ([]string, error)
	// Grant durably authorizes tool for workspaceID.
	Grant(ctx context.Context, workspaceID, tool string) error
	// IsGranted reports whether tool is durably authorized for workspaceID.
	// Used by the approval slow-path to short-circuit a prompt when a prior
	// "always" grant already exists in durable storage.
	IsGranted(ctx context.Context, workspaceID, tool string) (bool, error)
}

// DynamicPolicy augments a base policy with runtime grants from the approval
// flow: in-memory grants (session and hydrated "always") plus an optional
// GrantStore for durable persistence. Grants are keyed by (workspace, tool).
// Safe for concurrent use.
type DynamicPolicy struct {
	base    Policy
	store   GrantStore // optional (nil → session-only)
	mu      sync.RWMutex
	granted map[string]bool // workspace\x00tool
}

func NewDynamicPolicy(base Policy, store GrantStore) *DynamicPolicy {
	return &DynamicPolicy{base: base, store: store, granted: map[string]bool{}}
}

func grantKey(workspaceID, tool string) string { return workspaceID + "\x00" + tool }

func (p *DynamicPolicy) Decide(req Request) Decision {
	if p.base != nil {
		if d := p.base.Decide(req); d.Allowed() {
			return d
		}
	}
	p.mu.RLock()
	granted := p.granted[grantKey(req.Addr.WorkspaceID, req.Name)]
	p.mu.RUnlock()
	if granted {
		return Decision{Code: DecisionAllow, Reason: "approved by user (granted)", AuditCode: "tool.user_grant"}
	}
	return Decision{Code: DecisionRequiresApproval, Reason: "tool is not pre-authorized", AuditCode: "tool.approval_required"}
}

// GrantSession records an in-memory grant for the rest of this session/process.
func (p *DynamicPolicy) GrantSession(workspaceID, tool string) {
	p.mu.Lock()
	p.granted[grantKey(workspaceID, tool)] = true
	p.mu.Unlock()
}

// GrantAlways persists a durable per-workspace grant (and covers the session).
// Degrades to a session grant when no store is configured.
func (p *DynamicPolicy) GrantAlways(ctx context.Context, workspaceID, tool string) error {
	p.GrantSession(workspaceID, tool)
	if p.store == nil {
		return nil
	}
	return p.store.Grant(ctx, workspaceID, tool)
}

// Hydrate loads a workspace's durable "always" grants into memory (called once
// per workspace before serving tools). No-op without a store.
func (p *DynamicPolicy) Hydrate(ctx context.Context, workspaceID string) error {
	if p.store == nil {
		return nil
	}
	tools, err := p.store.ListGranted(ctx, workspaceID)
	if err != nil {
		return err
	}
	p.mu.Lock()
	for _, t := range tools {
		p.granted[grantKey(workspaceID, t)] = true
	}
	p.mu.Unlock()
	return nil
}

type AuditRecord struct {
	CallID        string       `json:"call_id"`
	ToolName      string       `json:"tool_name"`
	Decision      DecisionCode `json:"decision"`
	Reason        string       `json:"reason"`
	AuditCode     string       `json:"audit_code"`
	ArgumentsHash string       `json:"arguments_hash"`
}

func NewAuditRecord(req Request, decision Decision) AuditRecord {
	sum := sha256.Sum256(req.Arguments)
	return AuditRecord{
		CallID:        req.CallID,
		ToolName:      req.Name,
		Decision:      decision.Code,
		Reason:        decision.Reason,
		AuditCode:     decision.AuditCode,
		ArgumentsHash: hex.EncodeToString(sum[:]),
	}
}

func policyError(decision Decision, name string) error {
	return fmt.Errorf("%w: %s: %s", ErrToolRequiresApproval, name, decision.Reason)
}
