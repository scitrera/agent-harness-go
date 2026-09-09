// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package configpath defines the operator-supplied path conventions shared by
// the OSS executable and distributions built on it.
package configpath

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ParseList parses the canonical comma-separated directory-list syntax. Order
// is significant: filesystem discovery uses first occurrence wins.
func ParseList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// ValidateAbsolute requires every configured system/operator root to be
// absolute. Missing roots are valid and may be skipped by discovery, but a
// relative root is ambiguous because it could resolve against either the
// process cwd or the selected workspace.
func ValidateAbsolute(label string, dirs []string) error {
	for _, dir := range dirs {
		if !filepath.IsAbs(dir) {
			return fmt.Errorf("%s entry %q must be an absolute path", label, dir)
		}
	}
	return nil
}

// ValidateRelative requires workspace-owned roots to remain relative to the
// selected workspace. Operator-owned absolute roots belong in the matching
// system directory list, which has different precedence and access semantics.
func ValidateRelative(label string, dirs []string) error {
	for _, dir := range dirs {
		if filepath.IsAbs(dir) {
			return fmt.Errorf("%s entry %q must be workspace-relative", label, dir)
		}
		clean := filepath.Clean(dir)
		if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%s entry %q escapes the workspace", label, dir)
		}
	}
	return nil
}

// ResolveFile resolves an operator-supplied file against the workspace when it
// is relative. Absolute paths are returned cleaned and otherwise unchanged.
func ResolveFile(workspaceRoot, configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return ""
	}
	if filepath.IsAbs(configured) {
		return filepath.Clean(configured)
	}
	return filepath.Join(workspaceRoot, configured)
}
