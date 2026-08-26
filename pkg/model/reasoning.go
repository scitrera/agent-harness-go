// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package model

import (
	"fmt"
	"strings"
)

// ReasoningConfig controls request-scoped reasoning effort for one model.
// AllowedEfforts is optional: an empty list means the registry imposes no
// model-specific restriction beyond the neutral known-value validation.
type ReasoningConfig struct {
	DefaultEffort  string   `json:"default_effort,omitempty" yaml:"default_effort,omitempty"`
	AllowedEfforts []string `json:"allowed_efforts,omitempty" yaml:"allowed_efforts,omitempty"`
}

var knownReasoningEfforts = map[string]struct{}{
	"none": {}, "minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {}, "max": {},
}

var reasoningEffortOrder = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}

// KnownReasoningEfforts returns the provider-neutral effort vocabulary in
// display order. A model allowlist can narrow this set.
func KnownReasoningEfforts() []string {
	return append([]string(nil), reasoningEffortOrder...)
}

// NormalizeReasoningEffort canonicalizes a user/config value. "med" is a
// convenience alias; the provider wire value remains "medium".
func NormalizeReasoningEffort(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "", nil
	}
	if value == "med" {
		value = "medium"
	}
	if _, ok := knownReasoningEfforts[value]; !ok {
		return "", fmt.Errorf("unknown reasoning effort %q (expected none, minimal, low, medium, high, xhigh, or max)", value)
	}
	return value, nil
}

// Allows reports whether an already-normalized effort is admitted by the
// model-specific allowlist. An empty allowlist is intentionally unrestricted.
func (c ReasoningConfig) Allows(effort string) bool {
	if effort == "" || len(c.AllowedEfforts) == 0 {
		return true
	}
	for _, allowed := range c.AllowedEfforts {
		normalized, err := NormalizeReasoningEffort(allowed)
		if err == nil && normalized == effort {
			return true
		}
	}
	return false
}

func normalizeReasoningConfig(config ReasoningConfig) (ReasoningConfig, error) {
	defaultEffort, err := NormalizeReasoningEffort(config.DefaultEffort)
	if err != nil {
		return ReasoningConfig{}, fmt.Errorf("default_effort: %w", err)
	}
	allowed := make([]string, 0, len(config.AllowedEfforts))
	seen := map[string]struct{}{}
	for index, value := range config.AllowedEfforts {
		normalized, err := NormalizeReasoningEffort(value)
		if err != nil || normalized == "" {
			if err == nil {
				err = fmt.Errorf("reasoning effort is empty")
			}
			return ReasoningConfig{}, fmt.Errorf("allowed_efforts[%d]: %w", index, err)
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		allowed = append(allowed, normalized)
	}
	result := ReasoningConfig{DefaultEffort: defaultEffort, AllowedEfforts: allowed}
	if defaultEffort != "" && !result.Allows(defaultEffort) {
		return ReasoningConfig{}, fmt.Errorf("default_effort %q is not present in allowed_efforts", defaultEffort)
	}
	return result, nil
}
