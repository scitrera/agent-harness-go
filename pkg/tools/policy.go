package tools

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
