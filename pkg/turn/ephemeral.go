package turn

import "context"

// ephemeralCtxKey marks a turn as EPHEMERAL (one-shot synthesis). When set, the
// runner loads NO prior durable history (context = only the inbound message +
// its parts), forces memory recall+commit OFF (regardless of runner config), and
// persists nothing to the durable store (no session Append/SaveHistory, no
// backend thread mint). The distribution sets it per turn from the inbound
// signal (sahara: meta.scitrera.ephemeral); oss stays neutral about the source.
type ephemeralCtxKey struct{}

// WithEphemeral returns ctx marked as an ephemeral one-shot turn. The transport/
// distribution sets it when an inbound turn must not read or write durable state.
func WithEphemeral(ctx context.Context) context.Context {
	return context.WithValue(ctx, ephemeralCtxKey{}, true)
}

// EphemeralFrom reports whether the turn carried on ctx is ephemeral.
func EphemeralFrom(ctx context.Context) bool {
	v, _ := ctx.Value(ephemeralCtxKey{}).(bool)
	return v
}

type excludedToolsCtxKey struct{}

// WithExcludedTools scopes a turn's tool set: the named tools are neither
// advertised to the model (dropped from the assembled specs) NOR executable
// (invokeTool rejects them). A general per-turn seam — the distribution uses it
// to restrict a one-shot/ephemeral turn (e.g. excluding spawn_subagent so an
// ephemeral turn can't spawn durable child threads). Empty names → no-op.
func WithExcludedTools(ctx context.Context, names []string) context.Context {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	if len(set) == 0 {
		return ctx
	}
	return context.WithValue(ctx, excludedToolsCtxKey{}, set)
}

// excludedTools returns the per-turn tool-exclusion set carried on ctx (nil = none).
func excludedTools(ctx context.Context) map[string]struct{} {
	set, _ := ctx.Value(excludedToolsCtxKey{}).(map[string]struct{})
	return set
}

// toolExcluded reports whether name is scoped out of this turn.
func toolExcluded(ctx context.Context, name string) bool {
	_, ok := excludedTools(ctx)[name]
	return ok
}
