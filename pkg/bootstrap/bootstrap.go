// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package bootstrap defines workspace bootstrap context files (SOUL.md,
// AGENTS.md, IDENTITY.md, …) injected into the system prompt, and the Loader
// seam that supplies them. The type is protocol-neutral; the core loads them
// from the filesystem, the Scitrera distribution from MemoryLayer.
package bootstrap

import "context"

// File is a single workspace bootstrap document.
type File struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// Loader supplies the bootstrap files for a turn.
type Loader interface {
	LoadBootstrap(ctx context.Context) ([]File, error)
}
