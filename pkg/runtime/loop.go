// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/steering"
)

const (
	defaultPollInterval = time.Second
	defaultErrorBackoff = 2 * time.Second
	defaultMaxBackoff   = 30 * time.Second
)

type LoopConfig struct {
	PollInterval time.Duration
	ErrorBackoff time.Duration
	MaxBackoff   time.Duration
	MaxTurns     int
	// Concurrency > 1 runs turns concurrently across thread ids (serialized
	// within a thread), bounded by this many simultaneous turns. <= 1 is serial.
	Concurrency int
}

type LoopStats struct {
	Turns     int `json:"turns"`
	Errors    int `json:"errors"`
	Cancelled int `json:"cancelled"`
}

func (r *Runner) RunLoop(ctx context.Context, cfg LoopConfig) (LoopStats, error) {
	cfg = normalizeLoopConfig(cfg)
	if cfg.Concurrency > 1 {
		return r.runLoopConcurrent(ctx, cfg)
	}
	stats := LoopStats{}
	errorBackoff := cfg.ErrorBackoff
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		_, err := r.RunOnce(ctx)
		switch {
		case err == nil:
			stats.Turns++
			errorBackoff = cfg.ErrorBackoff
			if cfg.MaxTurns > 0 && stats.Turns >= cfg.MaxTurns {
				return stats, nil
			}
		case errors.Is(err, channel.ErrNoTask):
			if err := waitLoop(ctx, cfg.PollInterval); err != nil {
				return stats, err
			}
		case errors.Is(err, context.Canceled) && ctx.Err() == nil:
			// A per-turn cancel (the loop ctx is still live). Count it as a
			// completed-but-cancelled turn; do not back off.
			stats.Turns++
			stats.Cancelled++
			errorBackoff = cfg.ErrorBackoff
			if cfg.MaxTurns > 0 && stats.Turns >= cfg.MaxTurns {
				return stats, nil
			}
		default:
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			stats.Errors++
			if err := waitLoop(ctx, errorBackoff); err != nil {
				return stats, err
			}
			errorBackoff = nextBackoff(errorBackoff, cfg.MaxBackoff)
		}
	}
}

// runLoopConcurrent fetches tasks on this goroutine and dispatches them to a
// keyed worker pool: turns for different thread ids run concurrently (bounded by
// cfg.Concurrency), turns for the same thread run in order. Per-turn errors are
// recorded as stats and never stop the loop; only fetch/context errors do.
func (r *Runner) runLoopConcurrent(ctx context.Context, cfg LoopConfig) (LoopStats, error) {
	var mu sync.Mutex
	stats := LoopStats{}
	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err == nil:
			stats.Turns++
		case errors.Is(err, context.Canceled):
			stats.Turns++
			stats.Cancelled++
		default:
			stats.Errors++
		}
	}

	dispatcher := newKeyedDispatcher(cfg.Concurrency, func(env channel.Inbound) {
		_, err := r.runTask(ctx, env)
		record(err)
	}, threadKey)

	errorBackoff := cfg.ErrorBackoff
	submitted := 0
	for ctx.Err() == nil {
		if cfg.MaxTurns > 0 && submitted >= cfg.MaxTurns {
			break
		}
		env, err := r.source.FetchTask(ctx)
		switch {
		case err == nil:
			errorBackoff = cfg.ErrorBackoff
			// A steering send joins the turn already running on its thread, or is
			// rejected. Either way it never becomes a turn, so neither outcome is
			// counted as submitted.
			switch r.classifySteering(env) {
			case steeringParked:
				continue
			case steeringMissed:
				if fallback, ok := shellSteeringFallback(env); ok {
					dispatcher.submit(fallback)
					submitted++
					continue
				}
				r.rejectSteering(ctx, env, reasonNoActiveTurn)
				continue
			}
			dispatcher.submit(env)
			submitted++
		case errors.Is(err, channel.ErrNoTask):
			if waitLoop(ctx, cfg.PollInterval) != nil {
				dispatcher.wait()
				return stats, nil
			}
		default:
			if ctx.Err() != nil {
				dispatcher.wait()
				return stats, nil
			}
			if waitLoop(ctx, errorBackoff) != nil {
				dispatcher.wait()
				return stats, nil
			}
			errorBackoff = nextBackoff(errorBackoff, cfg.MaxBackoff)
		}
	}
	dispatcher.wait()
	return stats, nil
}

// threadKey is the dispatcher lane for a turn. It delegates to steering.Key so
// the lane a message is parked under and the lane its turn runs on cannot drift
// apart — a divergence would not fail loudly, it would just silently never
// deliver steering. (steering.Key length-prefixes the workspace so arbitrary
// opaque IDs cannot make two composite (workspace, thread) pairs share a lane.)
func threadKey(env channel.Inbound) string {
	addr := turnAddress(env)
	return steering.ConversationKey(addr)
}

func normalizeLoopConfig(cfg LoopConfig) LoopConfig {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.ErrorBackoff <= 0 {
		cfg.ErrorBackoff = defaultErrorBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.MaxBackoff < cfg.ErrorBackoff {
		cfg.MaxBackoff = cfg.ErrorBackoff
	}
	return cfg
}

func waitLoop(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextBackoff(current time.Duration, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next <= current || next > maxBackoff {
		return maxBackoff
	}
	return next
}
