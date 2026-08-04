package contextpack

import (
	"context"
	"testing"
)

func TestAppendSystemPromptExtraPreservesAndDeduplicatesSections(t *testing.T) {
	ctx := WithSystemPromptExtra(context.Background(), "caller instruction")
	ctx = AppendSystemPromptExtra(ctx, "working directory instruction")
	ctx = AppendSystemPromptExtra(ctx, "working directory instruction")

	if got, want := systemPromptExtraFrom(ctx), "caller instruction\n\nworking directory instruction"; got != want {
		t.Fatalf("prompt extra = %q, want %q", got, want)
	}
}
