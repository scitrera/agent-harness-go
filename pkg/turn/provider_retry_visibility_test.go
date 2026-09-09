// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// retryEvents filters an observer's record down to the provider-retry phase.
func retryEvents(events []hooks.TurnEvent) []hooks.TurnEvent {
	var out []hooks.TurnEvent
	for _, ev := range events {
		if ev.Phase == hooks.PhaseProviderRetry {
			out = append(out, ev)
		}
	}
	return out
}

// fixedBackoff makes the planned wait predictable so the test can assert the
// announced value equals the wait actually taken, not merely "something".
func fixedBackoff(attempt int) time.Duration {
	return time.Duration(attempt) * time.Millisecond
}

func Test_Runner_announces_transient_retries_with_attempt_and_wait(t *testing.T) {
	// Given: two transient failures then success.
	prov := &recoveryProvider{fail: func(call int, _ string) error {
		if call <= 2 {
			return &provider.ProviderError{Kind: provider.FailureRateLimit, Status: 429, Body: "slow down"}
		}
		return nil
	}}
	obs := &recordingTurnObserver{}
	r, err := NewRunner(Config{
		Store:         &fakeStore{},
		Loader:        fakeLoader{},
		Provider:      prov,
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		RetryBackoff:  fixedBackoff,
		TurnObservers: []hooks.TurnObserver{obs},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then: one announcement per retry, each carrying the ordinal and the wait
	// the runner is about to observe. Without these the session just goes quiet.
	got := retryEvents(obs.events)
	if len(got) != 2 {
		t.Fatalf("provider retry events = %d, want 2", len(got))
	}
	for i, ev := range got {
		wantAttempt := i + 1
		if ev.Attempt != wantAttempt {
			t.Fatalf("event %d attempt = %d, want %d", i, ev.Attempt, wantAttempt)
		}
		if ev.RetryIn != fixedBackoff(wantAttempt) {
			t.Fatalf("event %d retry_in = %v, want %v", i, ev.RetryIn, fixedBackoff(wantAttempt))
		}
		if ev.Strategy != hooks.RetryBackoff {
			t.Fatalf("event %d strategy = %q, want %q", i, ev.Strategy, hooks.RetryBackoff)
		}
		if ev.FailureKind != string(provider.FailureRateLimit) {
			t.Fatalf("event %d failure kind = %q, want %q", i, ev.FailureKind, provider.FailureRateLimit)
		}
		if ev.Err == nil {
			t.Fatalf("event %d carries no error", i)
		}
	}
}

func Test_Runner_announces_model_fallback_with_the_next_model(t *testing.T) {
	// Given: a non-retryable failure on the primary, success on the fallback.
	prov := &recoveryProvider{fail: func(_ int, model string) error {
		if model == "primary" {
			return &provider.ProviderError{Kind: provider.FailureServer, Status: 500, Body: "boom"}
		}
		return nil
	}}
	obs := &recordingTurnObserver{}
	r, err := NewRunner(Config{
		Store:               &fakeStore{},
		Loader:              fakeLoader{},
		Provider:            prov,
		Assembler:           contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		RetryBackoff:        noBackoff,
		MaxTransientRetries: -1, // skip the backoff ladder; go straight to fallback
		Model:               "primary",
		ModelRegistry:       twoModelRegistry(),
		ModelSelector:       fallbackSelector{to: "fast"},
		TurnObservers:       []hooks.TurnObserver{obs},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then: a model switch is a different user-facing story from a wait, so it
	// must be distinguishable and must name where the turn went.
	got := retryEvents(obs.events)
	if len(got) != 1 {
		t.Fatalf("provider retry events = %d, want 1", len(got))
	}
	if got[0].Strategy != hooks.RetryFallbackModel {
		t.Fatalf("strategy = %q, want %q", got[0].Strategy, hooks.RetryFallbackModel)
	}
	if got[0].Model != "primary" || got[0].NextModel != "fast" {
		t.Fatalf("switch = %q -> %q, want primary -> fast", got[0].Model, got[0].NextModel)
	}
	if got[0].RetryIn != 0 {
		t.Fatalf("retry_in = %v, want 0 for an immediate model switch", got[0].RetryIn)
	}
}

func Test_Runner_does_not_announce_a_retry_when_the_call_succeeds(t *testing.T) {
	// Given
	prov := &recoveryProvider{fail: func(int, string) error { return nil }}
	obs := &recordingTurnObserver{}
	r, err := NewRunner(Config{
		Store:         &fakeStore{},
		Loader:        fakeLoader{},
		Provider:      prov,
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		RetryBackoff:  noBackoff,
		TurnObservers: []hooks.TurnObserver{obs},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then
	if got := retryEvents(obs.events); len(got) != 0 {
		t.Fatalf("provider retry events = %d, want none on a clean call", len(got))
	}
}

func Test_providerRetryPart_carries_a_stable_id_and_a_standalone_message(t *testing.T) {
	// Given a retry the channel must render.
	part, err := providerRetryPart(ProviderRetryPayload{
		Attempt: 2, RetryInMS: 4000, Strategy: string(hooks.RetryBackoff),
		FailureKind: "rate_limit", Model: "primary",
		Message: retryMessage(hooks.TurnEvent{
			Strategy: hooks.RetryBackoff, FailureKind: "rate_limit",
			Model: "primary", Attempt: 2, RetryIn: 4 * time.Second,
		}),
	})
	if err != nil {
		t.Fatalf("build part: %v", err)
	}

	// When
	var decoded struct {
		Type    string               `json:"type"`
		Kind    string               `json:"kind"`
		ID      string               `json:"id"`
		Payload ProviderRetryPayload `json:"payload"`
	}
	if err := json.Unmarshal(part.Raw(), &decoded); err != nil {
		t.Fatalf("decode part: %v", err)
	}

	// Then: the id must be stable so successive attempts update one notice
	// instead of stacking a banner per attempt...
	if decoded.ID != providerRetryPartID {
		t.Fatalf("part id = %q, want %q", decoded.ID, providerRetryPartID)
	}
	if partID(part) != providerRetryPartID {
		t.Fatalf("emitter cannot key on the id: partID = %q", partID(part))
	}
	if decoded.Kind != DynamicProviderRetry {
		t.Fatalf("kind = %q, want %q", decoded.Kind, DynamicProviderRetry)
	}
	if decoded.Payload.RetryInMS != 4000 || decoded.Payload.Attempt != 2 {
		t.Fatalf("payload lost the countdown inputs: %#v", decoded.Payload)
	}
	// ...and the message must stand on its own, because a channel that does not
	// know this dynamic kind would otherwise render nothing at all.
	if decoded.Payload.Message == "" {
		t.Fatal("payload carries no fallback message")
	}
}
