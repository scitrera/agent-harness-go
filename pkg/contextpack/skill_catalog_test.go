package contextpack

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

func TestAssemblerUsesContextualWorkspaceSkillCatalog(t *testing.T) {
	assembler := NewAssembler(Config{
		Skills: []sysprompt.SkillSummary{{Name: "default-skill", Description: "default"}},
	})
	ctx := WithSkillCatalog(context.Background(), SkillCatalog{
		Skills: []sysprompt.SkillSummary{{Name: "project-skill", Description: "project only"}},
	})
	messages, err := assembler.Build(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) == 0 {
		t.Fatal("assembler returned no system message")
	}
	var text string
	for _, part := range messages[0].Content {
		if decoded, ok := part.AsText(); ok {
			text += decoded.Text
		}
	}
	if !strings.Contains(text, "project-skill") || strings.Contains(text, "default-skill") {
		t.Fatalf("workspace skill prompt = %q", text)
	}
}
