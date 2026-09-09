// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import "context"

// ModelPreferenceFunc requests that the thread switch to model `name` for
// subsequent turns. Best-effort: the runner applies it only if `name` is a
// registered/available model and returns whether it did. It pins the thread's
// model exactly like the /model command — effective from the NEXT turn, since the
// current turn's model is fixed at turn start. An empty name, no model registry,
// or an unknown model is a silent no-op.
//
// Like WorldStateSink and PartEmitter, this is a request-scoped capability the
// runner installs on ctx rather than threading a model-switch dependency through
// every tool signature. load_skill uses it to honor a skill's
// metadata.scitrera.preferred_model.
type ModelPreferenceFunc func(name string) bool

type modelPreferenceKey struct{}

// WithModelPreference returns a context carrying the per-turn model-preference fn.
func WithModelPreference(ctx context.Context, fn ModelPreferenceFunc) context.Context {
	return context.WithValue(ctx, modelPreferenceKey{}, fn)
}

// ModelPreferenceFrom returns the model-preference fn carried on ctx, and whether
// one was present. Absence is a no-op (e.g. a transport with no model registry).
func ModelPreferenceFrom(ctx context.Context) (ModelPreferenceFunc, bool) {
	fn, ok := ctx.Value(modelPreferenceKey{}).(ModelPreferenceFunc)
	return fn, ok
}
