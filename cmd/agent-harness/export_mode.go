package main

// export_mode.go implements `agent-harness --export`: read persisted thread
// history from <stateDir>/history and write it as JSONL (one thread per line) to
// stdout, in either an OpenAI chat shape (SFT-friendly) or a lossless spec-trace
// shape. It reads local state only — no provider/base-url needed — so it's a
// convenient way to pull training/analysis data out of a TUI/CLI session.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
)

type exportHistory struct {
	workspaceID string
	threadID    string
	messages    []protocol.ChatMessage
}

// runExport writes thread history under stateDir as JSONL to out. thread is a
// specific thread id or "all"; format is "openai" or "trace".
func runExport(stateDir, thread, format string, out io.Writer) error {
	if err := validateExportFormat(format); err != nil {
		return err
	}
	historyDir := filepath.Join(stateDir, "history")
	files, err := exportFiles(historyDir, thread)
	if err != nil {
		return err
	}
	histories := make([]exportHistory, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		var msgs []protocol.ChatMessage
		if err := json.Unmarshal(data, &msgs); err != nil {
			return fmt.Errorf("decode %s: %w", f, err)
		}
		if len(msgs) == 0 {
			continue
		}
		id := strings.TrimSuffix(filepath.Base(f), ".json")
		histories = append(histories, exportHistory{threadID: id, messages: msgs})
	}
	if len(histories) == 0 {
		return fmt.Errorf("no thread history to export under %s", historyDir)
	}
	return writeExport(histories, format, out)
}

// runWorkspaceExport is the composite-key counterpart to runExport. It uses
// FileStore's workspace-aware enumeration so callers never depend on its safe
// on-disk identifier encoding.
func runWorkspaceExport(stateDir, workspaceID, thread, format string, out io.Writer) error {
	if workspaceID == "" {
		return runExport(stateDir, thread, format, out)
	}
	if err := validateExportFormat(format); err != nil {
		return err
	}
	files := store.NewFileStore("", stateDir)
	var histories []exportHistory
	if thread == "all" {
		listed, err := files.ListWorkspaceHistory(context.Background(), workspaceID)
		if err != nil {
			return err
		}
		for _, history := range listed {
			histories = append(histories, exportHistory{workspaceID: workspaceID, threadID: history.ThreadID, messages: history.Messages})
		}
	} else {
		messages, err := files.LoadWorkspaceHistory(context.Background(), workspaceID, thread)
		if err != nil {
			return err
		}
		if len(messages) > 0 {
			histories = append(histories, exportHistory{workspaceID: workspaceID, threadID: thread, messages: messages})
		}
	}
	if len(histories) == 0 {
		return fmt.Errorf("no thread history to export for workspace %q", workspaceID)
	}
	return writeExport(histories, format, out)
}

func validateExportFormat(format string) error {
	switch format {
	case "openai", "trace":
		return nil
	default:
		return fmt.Errorf("unknown --export-format %q (want openai|trace)", format)
	}
}

func writeExport(histories []exportHistory, format string, out io.Writer) error {
	enc := json.NewEncoder(out)
	for _, history := range histories {
		var record any
		if format == "openai" {
			record = toOpenAIWorkspaceExport(history.workspaceID, history.threadID, history.messages)
		} else {
			record = toTraceWorkspaceExport(history.workspaceID, history.threadID, history.messages)
		}
		if err := enc.Encode(record); err != nil {
			return fmt.Errorf("encode %s: %w", history.threadID, err)
		}
	}
	return nil
}

// exportFiles resolves the history files to export: every *.json for "all",
// otherwise the single sanitized thread file.
func exportFiles(historyDir, thread string) ([]string, error) {
	if thread == "all" {
		entries, err := os.ReadDir(historyDir)
		if err != nil {
			return nil, fmt.Errorf("read history dir %s: %w", historyDir, err)
		}
		var files []string
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
				files = append(files, filepath.Join(historyDir, e.Name()))
			}
		}
		sort.Strings(files)
		if len(files) == 0 {
			return nil, fmt.Errorf("no thread history found under %s", historyDir)
		}
		return files, nil
	}
	path := filepath.Join(historyDir, sanitizeThread(thread)+".json")
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("thread %q history not found (%s): %w", thread, path, err)
	}
	return []string{path}, nil
}

// sanitizeThread mirrors store.sanitize (unexported there) so a single-thread
// export resolves to the same on-disk filename the FileStore wrote.
func sanitizeThread(id string) string {
	clean := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_").Replace(id)
	if clean == "" {
		return "default"
	}
	return clean
}

// exportUsage is the per-thread token accounting summed from each turn's
// meta[compaction.MetaUsage] record.
type exportUsage struct {
	Model            string `json:"model,omitempty"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
	Calls            int    `json:"calls"`
}

// sumUsage totals the per-turn usage records stamped on assistant messages.
func sumUsage(msgs []protocol.ChatMessage) exportUsage {
	var total exportUsage
	for _, m := range msgs {
		raw, ok := m.Meta[compaction.MetaUsage]
		if !ok || len(raw) == 0 {
			continue
		}
		var u exportUsage
		if json.Unmarshal(raw, &u) != nil {
			continue
		}
		total.PromptTokens += u.PromptTokens
		total.CompletionTokens += u.CompletionTokens
		total.TotalTokens += u.TotalTokens
		total.Calls += u.Calls
		if u.Model != "" {
			total.Model = u.Model // last turn's model wins
		}
	}
	return total
}

// --- OpenAI chat-completions export shape (SFT-friendly) ---

type oaiToolFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type oaiToolCall struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	Function oaiToolFunc `json:"function"`
}

type oaiMessage struct {
	Role       string        `json:"role"`
	Content    string        `json:"content"`
	ToolCalls  []oaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	Name       string        `json:"name,omitempty"`
}

type openAIExport struct {
	Workspace string       `json:"workspace,omitempty"`
	Thread    string       `json:"thread"`
	Messages  []oaiMessage `json:"messages"`
	Usage     exportUsage  `json:"usage"`
}

func toOpenAIExport(thread string, msgs []protocol.ChatMessage) openAIExport {
	return toOpenAIWorkspaceExport("", thread, msgs)
}

func toOpenAIWorkspaceExport(workspace, thread string, msgs []protocol.ChatMessage) openAIExport {
	out := make([]oaiMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, lowerToOAI(m)...)
	}
	return openAIExport{Workspace: workspace, Thread: thread, Messages: out, Usage: sumUsage(msgs)}
}

// lowerToOAI maps one persisted message to one or more OpenAI messages: text +
// tool_calls become a single assistant/user/system message; tool_result parts
// each become a "tool" message keyed by call id.
func lowerToOAI(m protocol.ChatMessage) []oaiMessage {
	var text strings.Builder
	var toolCalls []oaiToolCall
	var toolResults []oaiMessage
	for _, p := range m.Content {
		if tp, ok := p.AsText(); ok {
			text.WriteString(tp.Text)
			continue
		}
		if tc, ok := p.AsToolCall(); ok {
			toolCalls = append(toolCalls, oaiToolCall{
				ID:       tc.ID,
				Type:     "function",
				Function: oaiToolFunc{Name: tc.Name, Arguments: string(protocol.ArgsToRaw(tc.Args))},
			})
			continue
		}
		if tr, ok := p.AsToolResult(); ok {
			toolResults = append(toolResults, oaiMessage{
				Role:       "tool",
				ToolCallID: tr.CallID,
				Name:       tr.Name,
				Content:    string(tr.Output),
			})
			continue
		}
	}
	if len(toolResults) > 0 {
		return toolResults
	}
	role := string(m.Role)
	if role == string(protocol.RoleToolResult) {
		role = "tool"
	}
	return []oaiMessage{{Role: role, Content: text.String(), ToolCalls: toolCalls}}
}

// --- Lossless trace export shape ---

type traceExport struct {
	Workspace string                 `json:"workspace,omitempty"`
	Thread    string                 `json:"thread"`
	Messages  []protocol.ChatMessage `json:"messages"`
	Usage     exportUsage            `json:"usage"`
}

func toTraceExport(thread string, msgs []protocol.ChatMessage) traceExport {
	return toTraceWorkspaceExport("", thread, msgs)
}

func toTraceWorkspaceExport(workspace, thread string, msgs []protocol.ChatMessage) traceExport {
	return traceExport{Workspace: workspace, Thread: thread, Messages: msgs, Usage: sumUsage(msgs)}
}
