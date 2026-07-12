package sysprompt

import (
	"strings"
	"testing"
)

func TestBuildRequestInstructionsSection(t *testing.T) {
	p := Build(Input{RequestInstructions: "You are a summarizer. Output valid JSON only."})
	if !strings.Contains(p.DynamicSuffix, "## Request instructions") {
		t.Fatalf("request-instructions section missing: %q", p.DynamicSuffix)
	}
	if !strings.Contains(p.DynamicSuffix, "Output valid JSON only") {
		t.Fatalf("request-instructions text missing: %q", p.DynamicSuffix)
	}
	// It is the LAST suffix section (highest salience).
	if idx := strings.LastIndex(p.DynamicSuffix, "## "); !strings.HasPrefix(p.DynamicSuffix[idx:], "## Request instructions") {
		t.Errorf("request instructions should be the last section: %q", p.DynamicSuffix)
	}
	// Empty -> omitted.
	if off := Build(Input{}); strings.Contains(off.DynamicSuffix, "Request instructions") {
		t.Fatalf("empty request instructions must be omitted: %q", off.DynamicSuffix)
	}
}
