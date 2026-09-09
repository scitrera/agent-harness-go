// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package shellcontext identifies user-requested shell executions that should
// be preserved as conversational context. It is a dependency-free leaf so the
// TUI, remote transports, and turn runner share one wire representation.
package shellcontext

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const (
	// MetaKey carries the structured shell execution alongside its text form.
	MetaKey = "sahara.shell_context"
	// CommitAckMetaKey marks the empty final emitted after a context-only save.
	CommitAckMetaKey = "sahara.shell_context_committed"
)

// Record is the durable, provider-neutral description of one explicit shell
// execution. TriggerAgent is meaningful for idle sends; steering always enters
// an already-running turn.
type Record struct {
	Command         string `json:"command"`
	CWD             string `json:"cwd"`
	Output          string `json:"output,omitempty"`
	ExitCode        int    `json:"exit_code"`
	Error           string `json:"error,omitempty"`
	OutputBytes     int    `json:"output_bytes,omitempty"`
	OutputTruncated bool   `json:"output_truncated,omitempty"`
	TriggerAgent    bool   `json:"trigger_agent"`
}

// Put attaches a record to message metadata.
func Put(message *protocol.ChatMessage, record Record) error {
	if message == nil {
		return fmt.Errorf("shell context message is nil")
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode shell context: %w", err)
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[MetaKey] = raw
	return nil
}

// FromMessage returns the structured shell record when present and valid.
func FromMessage(message protocol.ChatMessage) (Record, bool) {
	raw, ok := message.Meta[MetaKey]
	if !ok {
		return Record{}, false
	}
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil || strings.TrimSpace(record.Command) == "" {
		return Record{}, false
	}
	return record, true
}

// IsContextOnly reports whether the message should be persisted without an LLM
// call. This is intentionally based on structured metadata, never prompt text.
func IsContextOnly(message protocol.ChatMessage) bool {
	record, ok := FromMessage(message)
	return ok && !record.TriggerAgent
}

// MarkCommitAck marks a transport final as acknowledgement-only.
func MarkCommitAck(message *protocol.ChatMessage) {
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[CommitAckMetaKey] = json.RawMessage(`true`)
}

// IsCommitAck reports whether a final only acknowledges a context save.
func IsCommitAck(message protocol.ChatMessage) bool {
	var committed bool
	raw, ok := message.Meta[CommitAckMetaKey]
	return ok && json.Unmarshal(raw, &committed) == nil && committed
}

// ContextText is the exact user-role text exposed to the model. The explicit
// envelope prevents command output from masquerading as user instructions.
func ContextText(record Record) string {
	payload := struct {
		CWD             string `json:"cwd"`
		Command         string `json:"command"`
		ExitCode        int    `json:"exit_code"`
		Output          string `json:"output"`
		Error           string `json:"error,omitempty"`
		OutputBytes     int    `json:"output_bytes,omitempty"`
		OutputTruncated bool   `json:"output_truncated,omitempty"`
	}{
		CWD: record.CWD, Command: record.Command, ExitCode: record.ExitCode,
		Output: record.Output, Error: record.Error, OutputBytes: record.OutputBytes,
		OutputTruncated: record.OutputTruncated,
	}
	raw, _ := json.MarshalIndent(payload, "", "  ")
	return "[shell_context]\n" + string(raw) + "\n[/shell_context]"
}

// DisplayText is the compact transcript form shown by the TUI.
func DisplayText(record Record) string {
	var lines []string
	lines = append(lines, "$ "+record.Command)
	if record.Output != "" {
		lines = append(lines, strings.TrimRight(record.Output, "\n"))
	}
	status := fmt.Sprintf("[exit %d", record.ExitCode)
	if record.OutputTruncated {
		status += ", output truncated"
	}
	status += "]"
	lines = append(lines, status)
	if record.Error != "" {
		lines = append(lines, record.Error)
	}
	return strings.Join(lines, "\n")
}
