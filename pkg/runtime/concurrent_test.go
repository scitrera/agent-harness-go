// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// queueSource yields a fixed list of task envelopes (one per FetchTask), then
// reports ErrNoTask. FetchTask is called only from the loop goroutine.
type queueSource struct {
	mu    sync.Mutex
	items []channel.Inbound
	i     int
}

func (s *queueSource) FetchTask(_ context.Context) (channel.Inbound, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.i >= len(s.items) {
		return channel.Inbound{}, channel.ErrNoTask
	}
	e := s.items[s.i]
	s.i++
	return e, nil
}

type execFunc func(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error)

func (f execFunc) Run(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
	return f(ctx, addr, user)
}

func envFor(thread, msgID string) channel.Inbound {
	addr := protocol.MessageAddress{ThreadID: thread, TaskID: msgID}
	return channel.Inbound{Addr: addr, Message: protocol.ChatMessage{ID: msgID, Addr: addr}}
}

func Test_RunLoop_concurrent_across_threads(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	exec := execFunc(func(ctx context.Context, addr protocol.MessageAddress, _ protocol.ChatMessage) (protocol.ChatMessage, error) {
		started <- addr.ThreadID
		select {
		case <-release:
		case <-ctx.Done():
			return protocol.ChatMessage{}, ctx.Err()
		}
		return protocol.ChatMessage{ID: "assistant", Role: protocol.RoleAssistant, Addr: addr}, nil
	})
	src := &queueSource{items: []channel.Inbound{envFor("t1", "u1"), envFor("t2", "u2"), envFor("t3", "u3")}}
	runner, err := NewRunner(src, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	done := make(chan LoopStats, 1)
	go func() {
		stats, _ := runner.RunLoop(context.Background(), LoopConfig{MaxTurns: 3, Concurrency: 3, PollInterval: time.Millisecond})
		done <- stats
	}()

	// All three turns must be simultaneously inside Run before any is released.
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case tid := <-started:
			seen[tid] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d turns started concurrently; expected 3", len(seen))
		}
	}
	close(release)

	stats := <-done
	if stats.Turns != 3 {
		t.Fatalf("expected 3 turns, got %+v", stats)
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct threads concurrent, got %v", seen)
	}
}

func Test_RunLoop_serializes_within_a_thread(t *testing.T) {
	var mu sync.Mutex
	active := 0
	maxActive := 0
	var order []string
	exec := execFunc(func(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		time.Sleep(20 * time.Millisecond) // window in which an overlap would show

		mu.Lock()
		order = append(order, user.ID)
		active--
		mu.Unlock()
		return protocol.ChatMessage{ID: "assistant", Role: protocol.RoleAssistant, Addr: addr}, nil
	})
	// Two tasks on the SAME thread, with headroom to run concurrently if allowed.
	src := &queueSource{items: []channel.Inbound{envFor("t1", "u1"), envFor("t1", "u2")}}
	runner, err := NewRunner(src, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	stats, err := runner.RunLoop(context.Background(), LoopConfig{MaxTurns: 2, Concurrency: 4, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("run loop: %v", err)
	}
	if stats.Turns != 2 {
		t.Fatalf("expected 2 turns, got %+v", stats)
	}
	if maxActive != 1 {
		t.Fatalf("same-thread turns must not overlap; maxActive=%d", maxActive)
	}
	if len(order) != 2 || order[0] != "u1" || order[1] != "u2" {
		t.Fatalf("expected in-order [u1 u2], got %v", order)
	}
}
