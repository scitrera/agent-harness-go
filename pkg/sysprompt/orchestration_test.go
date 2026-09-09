// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sysprompt

import (
	"strings"
	"testing"
)

// The delegation guidance appears only when spawn_subagent is actually available,
// so the prompt never promises a tool the model lacks.
func Test_orchestration_section_gated_on_spawn_subagent(t *testing.T) {
	const marker = "## Delegating context-heavy work"

	withSubagents := Build(Input{SubagentsEnabled: true}).Text()
	if !strings.Contains(withSubagents, marker) {
		t.Fatalf("expected delegation guidance when subagents are enabled:\n%s", withSubagents)
	}
	// It teaches the drive/inquire loop, not just that the tool exists.
	for _, want := range []string{"thread=<handle>", "keep YOUR context lean", "context window"} {
		if !strings.Contains(withSubagents, want) {
			t.Fatalf("delegation guidance missing %q", want)
		}
	}

	// Gated on the flag, NOT the (possibly-empty) Tools listing: a distribution may
	// leave Tools empty while still enabling subagents via the API tool-specs.
	withoutSubagents := Build(Input{Tools: []ToolSummary{{Name: "spawn_subagent"}}}).Text()
	if strings.Contains(withoutSubagents, marker) {
		t.Fatalf("delegation guidance must be gated on Subagents, not the Tools list:\n%s", withoutSubagents)
	}
}
