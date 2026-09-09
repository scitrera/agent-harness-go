// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package autonomy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSchedulerRunsJobsUntilStopped(t *testing.T) {
	fired := make(chan string, 4)
	s := NewScheduler(5 * time.Millisecond)
	s.Register(JobFunc{JobName: "tick", Fn: func(_ context.Context) error {
		select {
		case fired <- "tick":
		default:
		}
		return nil
	}})
	if !s.Start(context.Background()) {
		t.Fatal("Start should report active when interval > 0 and a job is registered")
	}
	defer s.Stop()

	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("job did not fire within timeout")
	}
}

func TestSchedulerDisabledIsNoop(t *testing.T) {
	// No jobs registered.
	if NewScheduler(time.Millisecond).Start(context.Background()) {
		t.Fatal("Start should be a no-op with no jobs")
	}
	// Non-positive interval.
	s := NewScheduler(0)
	s.Register(JobFunc{JobName: "x", Fn: func(context.Context) error { return nil }})
	if s.Start(context.Background()) {
		t.Fatal("Start should be a no-op with a non-positive interval")
	}
	s.Stop() // safe even though Start was a no-op
}

func TestSchedulerJobErrorDoesNotStopLoop(t *testing.T) {
	calls := make(chan struct{}, 8)
	s := NewScheduler(3 * time.Millisecond)
	s.Register(JobFunc{JobName: "boom", Fn: func(context.Context) error {
		calls <- struct{}{}
		return errors.New("boom")
	}})
	s.Start(context.Background())
	defer s.Stop()
	// Two fires prove the loop survived the first job error.
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatalf("expected fire %d after a job error", i+1)
		}
	}
}
