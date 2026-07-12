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
