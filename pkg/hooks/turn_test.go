// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"testing"
)

type turnObserverFunc func(context.Context, TurnEvent)

func (f turnObserverFunc) ObserveTurn(ctx context.Context, ev TurnEvent) { f(ctx, ev) }

func Test_turnPhaseToEvent_maps_only_supported_phases(t *testing.T) {
	supported := map[TurnPhase]EventName{
		PhaseUserPromptSubmit: EventUserPromptSubmit,
		PhaseCompacted:        EventPostCompact,
	}
	for phase, want := range supported {
		got, ok := turnPhaseToEvent(phase)
		if !ok || got != want {
			t.Fatalf("turnPhaseToEvent(%s) = (%s,%v), want (%s,true)", phase, got, ok, want)
		}
	}
	// Model-call + turn boundaries have no command-hook event and must not map
	// (the in-process TurnObserver still delivers them).
	for _, phase := range []TurnPhase{PhaseTurnStarted, PhaseModelCallStarted, PhaseModelCallFinished, PhaseTurnFinished} {
		if ev, ok := turnPhaseToEvent(phase); ok {
			t.Fatalf("phase %s must not map to a command-hook event, got %s", phase, ev)
		}
	}
}

func Test_NotifyTurn_fans_out_and_skips_nil(t *testing.T) {
	var seen int
	obs := turnObserverFunc(func(_ context.Context, _ TurnEvent) { seen++ })
	NotifyTurn(context.Background(), TurnEvent{Phase: PhaseTurnStarted}, []TurnObserver{obs, nil, obs})
	if seen != 2 {
		t.Fatalf("NotifyTurn delivered to %d observers, want 2 (nil skipped)", seen)
	}
}

func Test_Runtime_ObserveTurn_nil_is_safe(t *testing.T) {
	var rt *Runtime
	// Must not panic on a nil runtime (the "no hooks configured" path).
	rt.ObserveTurn(context.Background(), TurnEvent{Phase: PhaseUserPromptSubmit})
}
