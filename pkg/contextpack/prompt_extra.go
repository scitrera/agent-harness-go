package contextpack

import (
	"context"
	"strings"
)

type systemPromptExtraCtxKey struct{}

// WithSystemPromptExtra carries per-turn caller-supplied system-prompt text on ctx
// (e.g. an agent.synthesize one-shot's options.system/instructions). The assembler
// renders it as the last, highest-salience system-prompt section. A general seam —
// the distribution sets it per turn; empty text -> no-op. Mirrors the other
// per-turn ctx seams (WithEphemeral / WithExcludedTools).
func WithSystemPromptExtra(ctx context.Context, text string) context.Context {
	if text == "" {
		return ctx
	}
	return context.WithValue(ctx, systemPromptExtraCtxKey{}, text)
}

// AppendSystemPromptExtra adds a distinct, high-salience per-turn instruction
// without discarding one already installed by the caller. Re-adding the same
// instruction is a no-op, which matters for contexts inherited by subagents.
func AppendSystemPromptExtra(ctx context.Context, text string) context.Context {
	text = strings.TrimSpace(text)
	if text == "" {
		return ctx
	}
	current := strings.TrimSpace(systemPromptExtraFrom(ctx))
	if current == "" {
		return WithSystemPromptExtra(ctx, text)
	}
	for _, section := range strings.Split(current, "\n\n") {
		if strings.TrimSpace(section) == text {
			return ctx
		}
	}
	return WithSystemPromptExtra(ctx, current+"\n\n"+text)
}

// systemPromptExtraFrom returns the per-turn system-prompt extra on ctx ("" = none).
func systemPromptExtraFrom(ctx context.Context) string {
	s, _ := ctx.Value(systemPromptExtraCtxKey{}).(string)
	return s
}
