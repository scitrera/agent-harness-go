// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package skills

import "context"

type registryKey struct{}

// WithRegistry selects the workspace-resolved skill registry for load_skill on
// one turn. It does not mutate the runner's default registry.
func WithRegistry(ctx context.Context, registry *Registry) context.Context {
	if registry == nil {
		return ctx
	}
	return context.WithValue(ctx, registryKey{}, registry)
}

func registryFrom(ctx context.Context) (*Registry, bool) {
	registry, ok := ctx.Value(registryKey{}).(*Registry)
	return registry, ok && registry != nil
}
