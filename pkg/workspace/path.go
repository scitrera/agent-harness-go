// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package workspace

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"path/filepath"
)

// PathSegment produces a filesystem-safe, injective segment for ordinary
// opaque identifiers. Very large IDs use a full digest so the component remains
// below common filesystem limits.
func PathSegment(id string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(id))
	if encoded == "" {
		return "empty"
	}
	if len(encoded) <= 160 {
		return encoded
	}
	sum := sha256.Sum256([]byte(id))
	return "sha256-" + hex.EncodeToString(sum[:])
}

// StateDir returns the per-workspace child of base. Empty workspaceID preserves
// the legacy unscoped directory.
func StateDir(base, workspaceID string) string {
	if workspaceID == "" {
		return base
	}
	return filepath.Join(base, "workspaces", PathSegment(workspaceID))
}
