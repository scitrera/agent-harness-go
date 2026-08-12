package catalog

import (
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

// EntryResourceFamilyPattern returns the narrow Aether glob for every provider
// tool in one exact authenticated selector context. Only the final provider and
// tool slots are globs; user-controlled context values remain encoded segments.
func EntryResourceFamilyPattern(context spec.ToolCatalogContext) (string, error) {
	return entryResourcePath(context, "*", "*", true)
}

func entryResourcePath(context spec.ToolCatalogContext, provider, tool string, familyPattern bool) (string, error) {
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
		{"provider", provider, false},
		{"tool", tool, false},
	}
	encoded := make([]string, len(values))
	for i, value := range values {
		if familyPattern && (value.label == "provider" || value.label == "tool") {
			encoded[i] = "*"
			continue
		}
		segment, err := encodeResourceSegment(value.value, value.optional)
		if err != nil {
			return "", fmt.Errorf("%s: %w", value.label, err)
		}
		encoded[i] = segment
	}
	return fmt.Sprintf(
		"workspaces/%s/threads/%s/views/%s/hosts/%s/surfaces/%s/instances/%s/providers/%s/tools/%s",
		encoded[0], encoded[1], encoded[2], encoded[3], encoded[4], encoded[5], encoded[6], encoded[7],
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
