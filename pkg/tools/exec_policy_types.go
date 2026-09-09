// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import "errors"

var (
	ErrInvalidCommandPolicy = errors.New("tools: invalid command policy")
	ErrCommandPolicyFixture = errors.New("tools: command policy fixture failed")
)

type CommandDecider interface {
	DecideCommand(req CommandRequest) CommandDecision
}

type CommandPolicyConfig struct {
	Rules                  []CommandRule
	HostExecutables        []HostExecutable
	ResolveHostExecutables bool
	DefaultDecision        DecisionCode
}

type CommandRule struct {
	ID             string
	Decision       DecisionCode
	Reason         string
	AuditCode      string
	Pattern        CommandPattern
	HostExecutable string
	ReadOnly       bool
	Match          [][]string
	NotMatch       [][]string
	EnvAllowlist   []string
}

type CommandPattern struct {
	Tokens []CommandToken
}

type CommandToken struct {
	Literal      string
	Alternatives []string
}

func LiteralToken(value string) CommandToken {
	return CommandToken{Literal: value}
}

func AnyToken(values ...string) CommandToken {
	return CommandToken{Alternatives: append([]string(nil), values...)}
}

type HostExecutable struct {
	Name  string
	Paths []string
}

type CommandRequest struct {
	Argv          []string
	WorkspaceRoot string
	CWD           string
	Env           []string
}

type CommandDecision struct {
	Decision
	MatchedRuleID    string
	MatchedPrefix    []string
	MatchedRules     []CommandRuleMatch
	ReadOnly         bool
	HostExecutable   string
	ResolvedProgram  string
	WorkspaceRoot    string
	WorkingDirectory string
	EnvKeys          []string
}

type CommandRuleMatch struct {
	RuleID          string       `json:"rule_id"`
	MatchedPrefix   []string     `json:"matched_prefix"`
	Decision        DecisionCode `json:"decision"`
	Reason          string       `json:"reason"`
	AuditCode       string       `json:"audit_code"`
	ReadOnly        bool         `json:"read_only"`
	HostExecutable  string       `json:"host_executable,omitempty"`
	ResolvedProgram string       `json:"resolved_program,omitempty"`
}

type CommandPolicy struct {
	rules                  []compiledCommandRule
	hostExecutables        map[string]map[string]bool
	resolveHostExecutables bool
	defaultDecision        DecisionCode
}

type compiledCommandRule struct {
	rule CommandRule
}
