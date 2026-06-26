package tools

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// PartEmitter lets a tool surface a content part on the current assistant
// message's stream (e.g. a todo checklist the model writes via todo_write). The
// runner provides one per turn via the context; it streams the part and folds it
// into the finalized message so it persists. Like MemoryAuthority, it is a
// request-scoped capability carried on ctx rather than threaded through every
// tool signature.
type PartEmitter interface {
	// UpsertPart streams part: appended the first time its id is seen, or merged
	// in place (part_updated) on subsequent calls with the same id. A part with
	// no id is appended each call. The latest version of each id-bearing part is
	// folded into the finalized assistant message.
	UpsertPart(ctx context.Context, part protocol.ContentPart) error
}

type partEmitterKey struct{}

// WithPartEmitter returns a context carrying the per-turn PartEmitter.
func WithPartEmitter(ctx context.Context, e PartEmitter) context.Context {
	return context.WithValue(ctx, partEmitterKey{}, e)
}

// PartEmitterFrom returns the PartEmitter carried on ctx, and whether one was
// present. Tools that want to surface a part check this; absence means the part
// is simply not streamed (e.g. a non-streaming CLI path).
func PartEmitterFrom(ctx context.Context) (PartEmitter, bool) {
	e, ok := ctx.Value(partEmitterKey{}).(PartEmitter)
	return e, ok
}
