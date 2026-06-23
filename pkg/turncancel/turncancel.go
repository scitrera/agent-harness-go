// Package turncancel provides a small registry that maps an in-flight turn
// (keyed by task id) to its cancel func, so an out-of-band cancel control can
// abort the running turn's context. It has no dependencies on the runtime or
// transport packages, avoiding an import cycle between them.
package turncancel

import (
	"context"
	"sync"
)

// Canceller tracks cancel funcs for active turns by task id.
type Canceller struct {
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// New returns a ready Canceller.
func New() *Canceller {
	return &Canceller{active: map[string]context.CancelFunc{}}
}

// Begin derives a cancellable context for a turn and registers it under taskID.
// The returned done func deregisters and cancels; call it when the turn ends.
// A nil Canceller or empty taskID is a no-op (returns parent + a no-op done).
func (c *Canceller) Begin(parent context.Context, taskID string) (context.Context, func()) {
	if c == nil || taskID == "" {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	// If a prior turn for this id is somehow still registered, cancel it.
	if prev, ok := c.active[taskID]; ok {
		prev()
	}
	c.active[taskID] = cancel
	c.mu.Unlock()
	return ctx, func() {
		c.mu.Lock()
		if cur, ok := c.active[taskID]; ok {
			// Only delete if it's still our entry (avoid clobbering a newer turn).
			_ = cur
			delete(c.active, taskID)
		}
		c.mu.Unlock()
		cancel()
	}
}

// Cancel aborts the active turn for taskID. Returns true if one was found.
func (c *Canceller) Cancel(taskID string) bool {
	if c == nil || taskID == "" {
		return false
	}
	c.mu.Lock()
	cancel, ok := c.active[taskID]
	c.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}
