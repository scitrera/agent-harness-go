// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package approval

import (
	"context"
	"errors"
	"testing"
	"time"
)

func Test_Broker_Await_resolves(t *testing.T) {
	b := New()
	done := make(chan Decision, 1)
	go func() {
		d, err := b.Await(context.Background(), "task1", "appr_1")
		if err != nil {
			t.Errorf("await: %v", err)
		}
		done <- d
	}()
	// Give the goroutine a moment to register, then resolve.
	deadline := time.After(time.Second)
	for !b.Resolve("task1", "appr_1", Decision{Granted: true, Scope: "session"}) {
		select {
		case <-deadline:
			t.Fatal("Resolve never found the waiter")
		default:
		}
	}
	select {
	case d := <-done:
		if !d.Granted || d.Scope != "session" {
			t.Fatalf("decision = %#v", d)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not return after resolve")
	}
}

func Test_Broker_Await_returns_on_ctx_cancel(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Await(ctx, "task1", "appr_1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func Test_Broker_Resolve_no_waiter_is_noop(t *testing.T) {
	b := New()
	if b.Resolve("task1", "missing", Decision{Granted: true}) {
		t.Fatal("Resolve should return false when no waiter is pending")
	}
}

func Test_Broker_Await_distinct_requests_isolated(t *testing.T) {
	b := New()
	got := make(chan string, 2)
	for _, id := range []string{"a", "b"} {
		id := id
		go func() {
			d, _ := b.Await(context.Background(), "task1", id)
			got <- id + ":" + d.Scope
		}()
	}
	// Resolve only "a"; "b" must stay pending.
	deadline := time.After(time.Second)
	for !b.Resolve("task1", "a", Decision{Granted: true, Scope: "once"}) {
		select {
		case <-deadline:
			t.Fatal("could not resolve a")
		default:
		}
	}
	select {
	case v := <-got:
		if v != "a:once" {
			t.Fatalf("got %q, want a:once", v)
		}
	case <-time.After(time.Second):
		t.Fatal("a did not resolve")
	}
	select {
	case v := <-got:
		t.Fatalf("b should still be pending, got %q", v)
	case <-time.After(50 * time.Millisecond):
	}
}
