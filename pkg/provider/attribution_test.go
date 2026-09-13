// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestApplyAttributionHeadersStampsPerTurnIDs(t *testing.T) {
	ctx := WithAttribution(context.Background(), Attribution{
		Workspace: "ws-1",
		ThreadID:  "th-1",
		TaskID:    "tk-1",
	})
	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1/chat/completions", nil)

	applyAttributionHeaders(ctx, req)

	if got := req.Header.Get("X-Scitrera-Workspace"); got != "ws-1" {
		t.Fatalf("workspace header = %q, want ws-1", got)
	}
	if got := req.Header.Get("X-Scitrera-Thread-Id"); got != "th-1" {
		t.Fatalf("thread header = %q, want th-1", got)
	}
	if got := req.Header.Get("X-Scitrera-Task-Id"); got != "tk-1" {
		t.Fatalf("task header = %q, want tk-1", got)
	}
}

func TestApplyAttributionHeadersOmitsEmptyFields(t *testing.T) {
	// Only a thread id this turn — workspace/task absent → their headers unset.
	ctx := WithAttribution(context.Background(), Attribution{ThreadID: "th-1"})
	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1", nil)

	applyAttributionHeaders(ctx, req)

	if req.Header.Get("X-Scitrera-Thread-Id") != "th-1" {
		t.Fatalf("thread header missing")
	}
	if _, ok := req.Header["X-Scitrera-Workspace"]; ok {
		t.Fatalf("workspace header should be absent")
	}
	if _, ok := req.Header["X-Scitrera-Task-Id"]; ok {
		t.Fatalf("task header should be absent")
	}
}

func TestWithAttributionZeroIsNoop(t *testing.T) {
	base := context.Background()
	if WithAttribution(base, Attribution{}) != base {
		t.Fatalf("zero attribution must return the same context (no-op)")
	}
	// A request built from a no-attribution context gets no X-Scitrera-* headers.
	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1", nil)
	applyAttributionHeaders(base, req)
	for _, name := range []string{"X-Scitrera-Workspace", "X-Scitrera-Thread-Id", "X-Scitrera-Task-Id"} {
		if _, ok := req.Header[name]; ok {
			t.Fatalf("unexpected header %s on unattributed request", name)
		}
	}
}

func TestApplyAttributionHeadersNeverOverwrites(t *testing.T) {
	ctx := WithAttribution(context.Background(), Attribution{Workspace: "ws-from-ctx"})
	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1", nil)
	req.Header.Set("X-Scitrera-Workspace", "ws-preset")

	applyAttributionHeaders(ctx, req)

	if got := req.Header.Get("X-Scitrera-Workspace"); got != "ws-preset" {
		t.Fatalf("workspace header = %q, want preset value preserved", got)
	}
}

func TestConcurrentTurnUserAttribution(t *testing.T) {
	var pending sync.WaitGroup
	for _, user := range []string{"alice", "bob", "user:alice", "user:bob"} {
		pending.Add(1)
		go func(user string) {
			defer pending.Done()
			ctx := WithAttribution(context.Background(), Attribution{UserID: user, ThreadID: user})
			req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1", nil)
			applyAttributionHeaders(ctx, req)
			want := user
			if !strings.HasPrefix(want, "user:") {
				want = "user:" + want
			}
			if req.Header.Get("X-Scitrera-User") != want || req.Header.Get("X-Scitrera-Thread-Id") != user {
				t.Errorf("cross-turn attribution for %s: %v", user, req.Header)
			}
		}(user)
	}
	pending.Wait()
}
