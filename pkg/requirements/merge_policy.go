// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package requirements

import (
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func (s *mergeState) mergeApproval(layer Layer) error {
	if layer.Approval == nil || layer.Approval.Mode == ApprovalUnspecified {
		return nil
	}
	if err := validateApprovalMode(layer.Approval.Mode); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrInvalidLayer, layer.Source.display(), err)
	}
	incoming := withSource(*layer.Approval, layer.Source)
	if s.approval != nil && !equal(s.approval.Value, incoming.Value) {
		return conflict("approval.mode", s.approval.Source, layer.Source, "approval modes differ")
	}
	s.approval = &incoming
	return nil
}

func (s *mergeState) mergeToolPolicy(layer Layer) error {
	if layer.ToolPolicy == nil {
		return nil
	}
	policy := layer.ToolPolicy
	if policy.DefaultDecision != "" {
		if err := validateDecision(policy.DefaultDecision); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrInvalidLayer, layer.Source.display(), err)
		}
		incoming := withSource(policy.DefaultDecision, layer.Source)
		if s.defaultDecision != nil && s.defaultDecision.Value != incoming.Value {
			return conflict("tool_policy.default_decision", s.defaultDecision.Source, layer.Source, "default decisions differ")
		}
		s.defaultDecision = &incoming
	}
	if policy.ResolveHostExecutables != nil {
		incoming := withSource(*policy.ResolveHostExecutables, layer.Source)
		if s.resolveHostExecutables != nil && s.resolveHostExecutables.Value != incoming.Value {
			return conflict("tool_policy.resolve_host_executables", s.resolveHostExecutables.Source, layer.Source, "host executable resolution differs")
		}
		s.resolveHostExecutables = &incoming
	}
	for _, host := range policy.HostExecutables {
		if strings.TrimSpace(host.Name) == "" {
			return fmt.Errorf("%w: %s: host executable name required", ErrInvalidLayer, layer.Source.display())
		}
		incoming := withSource(host, layer.Source)
		existing, ok := s.hostByName[host.Name]
		if ok && !equal(existing.Value, host) {
			return conflict("tool_policy.host_executables."+host.Name, existing.Source, layer.Source, "host executable definitions differ")
		}
		if !ok {
			s.hostByName[host.Name] = incoming
			s.hosts = append(s.hosts, incoming)
		}
	}
	layerRules := make([]Sourced[tools.CommandRule], 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		if strings.TrimSpace(rule.ID) == "" {
			return fmt.Errorf("%w: %s: command rule id required", ErrInvalidLayer, layer.Source.display())
		}
		if err := validateRule(rule); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrInvalidLayer, layer.Source.display(), err)
		}
		incoming := withSource(rule, layer.Source)
		existing, ok := s.ruleByID[rule.ID]
		if ok && !equal(existing.Value, rule) {
			return conflict("tool_policy.rules."+rule.ID, existing.Source, layer.Source, "command rule definitions differ")
		}
		if !ok {
			s.ruleByID[rule.ID] = incoming
			layerRules = append(layerRules, incoming)
		}
	}
	s.rules = append(layerRules, s.rules...)
	return nil
}

func validateApprovalMode(mode ApprovalMode) error {
	switch mode {
	case ApprovalNever, ApprovalOnRequest, ApprovalAlways:
		return nil
	default:
		return fmt.Errorf("unsupported approval mode %q", mode)
	}
}

func validateDecision(decision tools.DecisionCode) error {
	switch decision {
	case tools.DecisionAllow, tools.DecisionRequiresApproval, tools.DecisionDeny:
		return nil
	default:
		return fmt.Errorf("unsupported decision %q", decision)
	}
}

func validateRule(rule tools.CommandRule) error {
	if err := validateDecision(rule.Decision); rule.Decision != "" && err != nil {
		return err
	}
	if len(rule.Pattern.Tokens) == 0 {
		return fmt.Errorf("command rule %q has empty pattern", rule.ID)
	}
	for i, token := range rule.Pattern.Tokens {
		if !validCommandToken(token) {
			return fmt.Errorf("command rule %q has invalid token %d", rule.ID, i)
		}
	}
	return nil
}

func validCommandToken(token tools.CommandToken) bool {
	if token.Literal != "" && len(token.Alternatives) == 0 {
		return true
	}
	if token.Literal != "" || len(token.Alternatives) == 0 {
		return false
	}
	for _, alt := range token.Alternatives {
		if alt == "" {
			return false
		}
	}
	return true
}
