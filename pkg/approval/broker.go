// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package approval is the human-in-the-loop tool-approval seam: a turn that
// hits a not-pre-authorized tool emits an approval_request and blocks on the
// Broker until an inbound approve/deny control resolves it (or the turn's
// context/timeout fires). It mirrors turncancel.Canceller — an inbound control
// reaching an in-flight turn — and is shared between the transport (which calls
// Resolve on inbound) and the turn runner (which calls Await).
package approval

import (
	"context"
	"sync"
)

// Decision is the outcome of an approval request.
type Decision struct {
	Granted bool
	// Scope is the grant breadth the user chose: "once" | "session" | "always".
	// Empty/ignored when Granted is false.
	Scope string
}

// Awaiter is the turn-facing surface: block until a decision for (taskID,
// requestID) arrives. The runner wraps the call in the approval timeout.
type Awaiter interface {
	Await(ctx context.Context, taskID, requestID string) (Decision, error)
}

// Broker correlates pending approval requests with inbound decisions, keyed by
// (taskID, requestID). Safe for concurrent use.
type Broker struct {
	mu      sync.Mutex
	waiters map[string]chan Decision
}

func New() *Broker {
	return &Broker{waiters: map[string]chan Decision{}}
}

func key(taskID, requestID string) string { return taskID + "\x00" + requestID }

// Await registers a waiter for (taskID, requestID) and blocks until Resolve
// delivers a decision or ctx is done (cancel/timeout). The waiter is always
// cleaned up. A nil broker blocks only on ctx (no decision can arrive).
func (b *Broker) Await(ctx context.Context, taskID, requestID string) (Decision, error) {
	k := key(taskID, requestID)
	ch := make(chan Decision, 1)
	b.mu.Lock()
	b.waiters[k] = ch
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.waiters, k)
		b.mu.Unlock()
	}()
	select {
	case d := <-ch:
		return d, nil
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	}
}

// Resolve delivers a decision to a pending Await for (taskID, requestID).
// Returns true if a waiter received it (false when none is pending — a stale or
// duplicate response, which is a harmless no-op).
func (b *Broker) Resolve(taskID, requestID string, d Decision) bool {
	b.mu.Lock()
	ch, ok := b.waiters[key(taskID, requestID)]
	b.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- d:
		return true
	default:
		return false // already resolved
	}
}
