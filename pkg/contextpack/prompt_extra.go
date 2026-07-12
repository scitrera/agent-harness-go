package contextpack

import "context"

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

// systemPromptExtraFrom returns the per-turn system-prompt extra on ctx ("" = none).
func systemPromptExtraFrom(ctx context.Context) string {
	s, _ := ctx.Value(systemPromptExtraCtxKey{}).(string)
	return s
}
