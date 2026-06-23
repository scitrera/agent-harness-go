// Package autonomy is the in-process foundation for proactive (cron-like)
// agent behavior. It runs registered jobs on a fixed interval. It is gated OFF
// by default at the config layer (host-configured; off unless the embedder enables it); the ecosystem
// direction is to queue/receive proactive work as Aether tasks, with this
// in-process layer as a bridge/fallback. See docs/cron-proactive-autonomy.md.
package autonomy

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Job is a unit of proactive work run on the scheduler's interval.
type Job interface {
	Name() string
	Run(ctx context.Context) error
}

// JobFunc adapts a function to Job.
type JobFunc struct {
	JobName string
	Fn      func(ctx context.Context) error
}

func (j JobFunc) Name() string                  { return j.JobName }
func (j JobFunc) Run(ctx context.Context) error { return j.Fn(ctx) }

// Scheduler runs its registered jobs every interval until stopped. A
// non-positive interval or an empty job set makes Start a no-op, so the
// disabled-by-default path costs nothing.
type Scheduler struct {
	interval time.Duration
	jobs     []Job

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewScheduler returns a scheduler that fires every interval.
func NewScheduler(interval time.Duration) *Scheduler {
	return &Scheduler{interval: interval}
}

// Register adds a job. Call before Start.
func (s *Scheduler) Register(job Job) {
	if job != nil {
		s.jobs = append(s.jobs, job)
	}
}

// Start launches the loop in a goroutine until ctx is cancelled or Stop is
// called. It is a no-op (and reports false) when disabled (interval <= 0 or no
// jobs), so callers can branch on activation.
func (s *Scheduler) Start(ctx context.Context) bool {
	if s.interval <= 0 || len(s.jobs) == 0 {
		return false
	}
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()
	go s.loop(ctx)
	return true
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, job := range s.jobs {
				if err := job.Run(ctx); err != nil {
					slog.WarnContext(ctx, "autonomy job failed", slog.String("job", job.Name()), slog.Any("err", err))
				}
			}
		}
	}
}

// Stop cancels the loop and waits for it to drain. Safe to call when Start was a
// no-op or already stopped.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
