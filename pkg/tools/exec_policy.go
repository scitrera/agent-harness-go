package tools

import (
	"fmt"
	"strings"
)

func NewCommandPolicy(cfg CommandPolicyConfig) (*CommandPolicy, error) {
	defaultDecision := cfg.DefaultDecision
	if defaultDecision == "" {
		defaultDecision = DecisionRequiresApproval
	}
	if err := validateDecision(defaultDecision); err != nil {
		return nil, err
	}

	policy := &CommandPolicy{
		rules:                  make([]compiledCommandRule, 0, len(cfg.Rules)),
		hostExecutables:        compileHostExecutables(cfg.HostExecutables),
		resolveHostExecutables: cfg.ResolveHostExecutables,
		defaultDecision:        defaultDecision,
	}
	for i, rule := range cfg.Rules {
		compiled, err := compileCommandRule(rule, i)
		if err != nil {
			return nil, err
		}
		policy.rules = append(policy.rules, compiled)
	}
	if err := policy.validateFixtures(); err != nil {
		return nil, err
	}
	return policy, nil
}

func (p *CommandPolicy) validateFixtures() error {
	for _, compiled := range p.rules {
		rule := compiled.rule
		for _, example := range rule.Match {
			if !compiled.matchesExact(example) {
				return fmt.Errorf("%w: rule %q match example %q did not match", ErrCommandPolicyFixture, rule.ID, strings.Join(example, " "))
			}
		}
		for _, example := range rule.NotMatch {
			if compiled.matchesExact(example) {
				return fmt.Errorf("%w: rule %q not_match example %q matched", ErrCommandPolicyFixture, rule.ID, strings.Join(example, " "))
			}
		}
	}
	return nil
}

func (p *CommandPolicy) envAllowlistForRule(ruleID string) []string {
	for _, rule := range p.rules {
		if rule.rule.ID == ruleID {
			return rule.rule.EnvAllowlist
		}
	}
	return nil
}

func compileCommandRule(rule CommandRule, _ int) (compiledCommandRule, error) {
	if len(rule.Pattern.Tokens) == 0 {
		return compiledCommandRule{}, fmt.Errorf("%w: command rule %q has empty pattern", ErrInvalidCommandPolicy, rule.ID)
	}
	decision := rule.Decision
	if decision == "" {
		decision = DecisionAllow
	}
	if err := validateDecision(decision); err != nil {
		return compiledCommandRule{}, err
	}
	rule.Decision = decision
	for i, token := range rule.Pattern.Tokens {
		if !token.valid() {
			return compiledCommandRule{}, fmt.Errorf("%w: command rule %q has invalid token %d", ErrInvalidCommandPolicy, rule.ID, i)
		}
	}
	return compiledCommandRule{rule: rule}, nil
}

func validateDecision(decision DecisionCode) error {
	switch decision {
	case DecisionAllow, DecisionRequiresApproval, DecisionDeny:
		return nil
	default:
		return fmt.Errorf("%w: unsupported decision %q", ErrInvalidCommandPolicy, decision)
	}
}
