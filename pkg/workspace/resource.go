package workspace

import (
	"fmt"
	"strings"
	"unicode/utf8"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	ExecutionViewResourceType  = "workspace-execution/view"
	ExecutionViewBindOperation = "bind"

	ExecutionViewReadAccess  = int32(10)
	ExecutionViewWriteAccess = int32(20)
)

// ExecutionViewResourceID returns the canonical Aether identity of one exact
// MemoryLayer workspace view hosted by one exact execution provider. Local
// roots and root_ref values are deliberately absent.
func ExecutionViewResourceID(binding spec.ExecutionBinding) (string, error) {
	if err := binding.Validate(); err != nil {
		return "", err
	}
	values := []struct {
		label string
		value string
	}{
		{label: "workspace", value: binding.WorkspaceID},
		{label: "view", value: binding.ViewID},
		{label: "host", value: binding.ToolHostID},
	}
	encoded := make([]string, len(values))
	for i, value := range values {
		segment, err := encodeExecutionResourceSegment(value.value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", value.label, err)
		}
		encoded[i] = segment
	}
	return fmt.Sprintf("workspaces/%s/views/%s/hosts/%s", encoded[0], encoded[1], encoded[2]), nil
}

// ExecutionViewRequiredAccess maps Sahara's local write ceiling to Aether's
// ordinary numeric access level. The operation remains the stable verb "bind";
// grants can narrow read/write with their access ceiling.
func ExecutionViewRequiredAccess(policy ExecutionViewPolicy) (int32, error) {
	if err := policy.Validate(); err != nil {
		return 0, err
	}
	if policy.WriteAccess == ViewWriteAccessReadWrite {
		return ExecutionViewWriteAccess, nil
	}
	return ExecutionViewReadAccess, nil
}

func encodeExecutionResourceSegment(value string) (string, error) {
	if value == "" {
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
