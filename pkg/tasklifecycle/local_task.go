package tasklifecycle

import (
	"encoding/json"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// LocalTaskMetaKey marks a turn whose task id the HARNESS minted for its own
// correlation (cancel, approvals) rather than receiving from an authoritative
// task host.
//
// The distinction matters wherever a host has a real task API: claiming a task
// the host never issued fails, and failing the claim fails the turn. A locally
// minted id is a correlation handle, not a claim on anything, so a host wraps
// with ExcludeLocal and leaves those turns alone.
const LocalTaskMetaKey = "local_task"

// MarkLocal flags a message whose task id was minted locally.
func MarkLocal(message protocol.ChatMessage) protocol.ChatMessage {
	if IsLocal(message) {
		return message
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[LocalTaskMetaKey] = json.RawMessage(`true`)
	return message
}

// IsLocal reports whether this turn's task id was minted locally.
func IsLocal(message protocol.ChatMessage) bool {
	raw, ok := message.Meta[LocalTaskMetaKey]
	if !ok {
		return false
	}
	var local bool
	if err := json.Unmarshal(raw, &local); err != nil {
		// An unreadable marker must not silently exempt a turn from its task
		// lifecycle: treat it as authoritative, which at worst surfaces a loud
		// claim failure rather than quietly skipping a terminal transition the
		// host is waiting for.
		return false
	}
	return local
}

// ExcludeLocal is the Predicate for hosts with an authoritative task API: manage
// every turn except those carrying a locally minted id.
func ExcludeLocal(_ protocol.MessageAddress, message protocol.ChatMessage) bool {
	return !IsLocal(message)
}
