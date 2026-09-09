// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// EntryResourceID is the canonical Aether resource path for one exact catalog
// entry in one authenticated selector context. Optional selector components use
// the reserved "!" sentinel; all user-controlled segments are percent-encoded
// with uppercase hexadecimal so every producer computes the same ACL key.
func EntryResourceID(context spec.ToolCatalogContext, ref spec.ToolReference) (string, error) {
	return entryResourcePath(context, ref.ProviderID, ref.Name, false)
}

// ProviderResourceID is the canonical Aether resource path for mutating one
// provider registration family in an authenticated selector context.
func ProviderResourceID(context spec.ToolCatalogContext, providerID string) (string, error) {
	base, err := catalogContextResourcePath(context)
	if err != nil {
		return "", err
	}
	provider, err := encodeResourceSegment(providerID, false)
	if err != nil {
		return "", fmt.Errorf("provider: %w", err)
	}
	return base + "/providers/" + provider, nil
}

// EntryResourceFamilyPattern returns the narrow Aether glob for every provider
// tool in one exact authenticated selector context. Only the final provider and
// tool slots are globs; user-controlled context values remain encoded segments.
func EntryResourceFamilyPattern(context spec.ToolCatalogContext) (string, error) {
	return entryResourcePath(context, "*", "*", true)
}

// MutationCorrelation binds a checked provider mutation receipt to its exact
// method, provider generation, and monotonic sequence.
func MutationCorrelation(action, providerID, registrationID, generation string, sequence uint64) string {
	raw := strings.Join([]string{action, providerID, registrationID, generation, fmt.Sprint(sequence)}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return "catalog:" + hex.EncodeToString(sum[:])
}

func entryResourcePath(context spec.ToolCatalogContext, provider, tool string, familyPattern bool) (string, error) {
	base, err := catalogContextResourcePath(context)
	if err != nil {
		return "", err
	}
	if familyPattern {
		return base + "/providers/*/tools/*", nil
	}
	providerSegment, err := encodeResourceSegment(provider, false)
	if err != nil {
		return "", fmt.Errorf("provider: %w", err)
	}
	toolSegment, err := encodeResourceSegment(tool, false)
	if err != nil {
		return "", fmt.Errorf("tool: %w", err)
	}
	return base + "/providers/" + providerSegment + "/tools/" + toolSegment, nil
}

func catalogContextResourcePath(context spec.ToolCatalogContext) (string, error) {
	values := []struct {
		label    string
		value    string
		optional bool
	}{
		{"workspace", context.WorkspaceID, false},
		{"thread", context.ThreadID, true},
		{"view", context.ViewID, true},
		{"host", context.ToolHostID, true},
		{"surface", context.SurfaceKind, true},
		{"instance", context.SurfaceInstanceID, true},
	}
	encoded := make([]string, len(values))
	for i, value := range values {
		segment, err := encodeResourceSegment(value.value, value.optional)
		if err != nil {
			return "", fmt.Errorf("%s: %w", value.label, err)
		}
		encoded[i] = segment
	}
	return fmt.Sprintf(
		"workspaces/%s/threads/%s/views/%s/hosts/%s/surfaces/%s/instances/%s",
		encoded[0], encoded[1], encoded[2], encoded[3], encoded[4], encoded[5],
	), nil
}

func encodeResourceSegment(value string, optional bool) (string, error) {
	if value == "" {
		if optional {
			return "!", nil
		}
		return "", fmt.Errorf("value is required")
	}
	if value == "!" {
		return "", fmt.Errorf("raw ! is reserved")
	}
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("value is not valid UTF-8")
	}
	const upperHex = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '.' || b == '_' || b == '~' {
			out.WriteByte(b)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(upperHex[b>>4])
		out.WriteByte(upperHex[b&0x0f])
	}
	return out.String(), nil
}
