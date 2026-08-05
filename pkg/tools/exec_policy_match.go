package tools

import (
	"path/filepath"
	"sort"
	"strings"
)

func (p *CommandPolicy) DecideCommand(req CommandRequest) CommandDecision {
	base := CommandDecision{
		Decision: Decision{
			Code:      p.defaultDecision,
			Reason:    defaultCommandReason(p.defaultDecision),
			AuditCode: defaultCommandAuditCode(p.defaultDecision),
		},
		WorkspaceRoot:    req.WorkspaceRoot,
		WorkingDirectory: req.CWD,
		EnvKeys:          envKeys(req.Env, nil),
	}
	if len(req.Argv) == 0 {
		base.Reason = "command argv is empty"
		return base
	}

	matches := p.matchesForArgv(req.Argv)
	if len(matches) == 0 {
		return base
	}

	out := base
	out.MatchedRules = matches
	best := matches[0]
	for _, candidate := range matches[1:] {
		if commandDecisionSeverity(candidate.Decision) > commandDecisionSeverity(best.Decision) {
			best = candidate
			continue
		}
		if candidate.Decision == best.Decision && len(candidate.MatchedPrefix) > len(best.MatchedPrefix) {
			best = candidate
		}
	}
	out.Decision = Decision{
		Code:      best.Decision,
		Reason:    best.Reason,
		AuditCode: best.AuditCode,
	}
	out.MatchedRuleID = best.RuleID
	out.MatchedPrefix = append([]string(nil), best.MatchedPrefix...)
	out.ReadOnly = best.ReadOnly
	out.HostExecutable = best.HostExecutable
	out.ResolvedProgram = best.ResolvedProgram
	out.EnvKeys = envKeys(req.Env, p.envAllowlistForRule(best.RuleID))
	return out
}

func (p *CommandPolicy) matchesForArgv(argv []string) []CommandRuleMatch {
	exact := make([]CommandRuleMatch, 0)
	for _, rule := range p.rules {
		if match, ok := rule.match(argv, ""); ok {
			exact = append(exact, match)
		}
	}
	if len(exact) > 0 || !p.resolveHostExecutables || !filepath.IsAbs(argv[0]) {
		return exact
	}

	program := filepath.Base(argv[0])
	if !p.hostExecutableAllowed(program, argv[0]) {
		return nil
	}
	fallbackArgv := append([]string{program}, argv[1:]...)
	fallback := make([]CommandRuleMatch, 0)
	for _, rule := range p.rules {
		if match, ok := rule.match(fallbackArgv, argv[0]); ok {
			fallback = append(fallback, match)
		}
	}
	return fallback
}

func (p *CommandPolicy) hostExecutableAllowed(name string, path string) bool {
	allowedPaths, ok := p.hostExecutables[name]
	if !ok {
		return true
	}
	return allowedPaths[path]
}

func (r compiledCommandRule) matchesExact(argv []string) bool {
	_, ok := r.match(argv, "")
	return ok
}

func (r compiledCommandRule) match(argv []string, resolvedProgram string) (CommandRuleMatch, bool) {
	if len(argv) < len(r.rule.Pattern.Tokens) {
		return CommandRuleMatch{}, false
	}
	for i, token := range r.rule.Pattern.Tokens {
		if !token.matches(argv[i]) {
			return CommandRuleMatch{}, false
		}
	}
	hostExecutable := r.rule.HostExecutable
	if hostExecutable == "" && resolvedProgram != "" {
		hostExecutable = filepath.Base(resolvedProgram)
	}
	matchedPrefix := append([]string(nil), argv[:len(r.rule.Pattern.Tokens)]...)
	return CommandRuleMatch{
		RuleID:          r.rule.ID,
		MatchedPrefix:   matchedPrefix,
		Decision:        r.rule.Decision,
		Reason:          commandRuleReason(r.rule),
		AuditCode:       commandRuleAuditCode(r.rule),
		ReadOnly:        r.rule.ReadOnly,
		HostExecutable:  hostExecutable,
		ResolvedProgram: resolvedProgram,
	}, true
}

func (t CommandToken) valid() bool {
	if t.Literal != "" && len(t.Alternatives) == 0 {
		return true
	}
	if t.Literal != "" || len(t.Alternatives) == 0 {
		return false
	}
	for _, alt := range t.Alternatives {
		if alt == "" {
			return false
		}
	}
	return true
}

func (t CommandToken) matches(value string) bool {
	if t.Literal != "" {
		return t.Literal == value
	}
	for _, alt := range t.Alternatives {
		if alt == value {
			return true
		}
	}
	return false
}

func compileHostExecutables(entries []HostExecutable) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, entry := range entries {
		if entry.Name == "" {
			continue
		}
		if _, ok := out[entry.Name]; !ok {
			out[entry.Name] = map[string]bool{}
		}
		for _, path := range entry.Paths {
			if path != "" {
				out[entry.Name][path] = true
			}
		}
	}
	return out
}

func commandDecisionSeverity(decision DecisionCode) int {
	switch decision {
	case DecisionDeny:
		return 3
	case DecisionRequiresApproval:
		return 2
	case DecisionAllow:
		return 1
	default:
		return 0
	}
}

func commandRuleReason(rule CommandRule) string {
	if rule.Reason != "" {
		return rule.Reason
	}
	return defaultCommandReason(rule.Decision)
}

func commandRuleAuditCode(rule CommandRule) string {
	if rule.AuditCode != "" {
		return rule.AuditCode
	}
	return defaultCommandAuditCode(rule.Decision)
}

func defaultCommandReason(decision DecisionCode) string {
	switch decision {
	case DecisionAllow:
		return "command is allowed by policy"
	case DecisionDeny:
		return "command is denied by policy"
	default:
		return "command requires approval"
	}
}

func defaultCommandAuditCode(decision DecisionCode) string {
	switch decision {
	case DecisionAllow:
		return "exec.allow"
	case DecisionDeny:
		return "exec.deny"
	default:
		return "exec.approval_required"
	}
}

func envKeys(env []string, allowlist []string) []string {
	allowed := map[string]bool{}
	for _, key := range allowlist {
		if key != "" {
			allowed[key] = true
		}
	}
	keys := make([]string, 0, len(env))
	seen := map[string]bool{}
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			key = entry
		}
		if key == "" || seen[key] {
			continue
		}
		if len(allowed) > 0 && !allowed[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
