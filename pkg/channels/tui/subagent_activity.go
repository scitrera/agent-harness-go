package tui

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const subagentUpdateLimit = 180

var subagentWorkingFrames = [...]string{
	"Subagent working ·",
	"Subagent working ··",
	"Subagent working ···",
	"Subagent working",
}

func (m *model) ensureSubagentMap() {
	if m.subagents == nil {
		m.subagents = map[string]subagentActivity{}
	}
}

func (m *model) applySubagentLifecycle(taskID string, event tools.ToolEvent) string {
	m.ensureSubagentMap()
	activity := m.subagents[event.CallID]
	if activity.CallID == "" {
		activity = subagentActivity{
			CallID:    event.CallID,
			TaskID:    taskID,
			Phase:     "working",
			StartedMS: time.Now().UnixMilli(),
		}
	}
	switch event.Status {
	case tools.ToolEventQueued, tools.ToolEventStarted:
		activity.Phase = "working"
	case tools.ToolEventAborted:
		activity.Phase = "failed"
		activity.Latest = event.ErrorMessage
	case tools.ToolEventFinished:
		if event.IsError {
			activity.Phase = "failed"
			activity.Latest = event.ErrorMessage
		}
	}
	m.subagents[event.CallID] = activity
	return m.renderSubagentActivity(activity)
}

func (m *model) applySubagentPart(taskID string, part protocol.SubagentPart) {
	m.ensureSubagentMap()
	callID := part.ID
	activity := m.subagents[callID]
	if activity.CallID == "" {
		callID, activity = m.findSubagentActivity(taskID, part.ThreadID)
	}
	if activity.CallID == "" {
		if callID == "" {
			callID = part.ID
		}
		activity = subagentActivity{CallID: callID, TaskID: taskID, StartedMS: time.Now().UnixMilli()}
	}
	activity.ThreadID = part.ThreadID
	activity.Name = part.Name
	if strings.TrimSpace(part.Summary) != "" {
		activity.Latest = part.Summary
	}
	switch part.Status {
	case protocol.SubagentPending, protocol.SubagentRunning:
		activity.Phase = "working"
	case protocol.SubagentCompleted:
		activity.Phase = "completed"
	case protocol.SubagentFailed:
		activity.Phase = "failed"
	case protocol.SubagentCancelled:
		activity.Phase = "cancelled"
	default:
		activity.Phase = "working"
	}
	m.subagents[activity.CallID] = activity
	m.upsertToolishRow(activity.CallID, m.renderSubagentActivity(activity), true)
}

func (m *model) applySubagentChildEvent(event channel.Event, childTool *tools.ToolEvent) {
	callID, activity := m.findSubagentActivity(event.Addr.TaskID, event.Addr.ThreadID)
	if activity.CallID == "" {
		return
	}
	activity.ThreadID = event.Addr.ThreadID
	switch event.Type {
	case channel.EventMessageStarted:
		activity.Output = ""
		activity.Latest = "thinking"
	case channel.EventTokenDelta:
		activity.Output += event.Delta
		activity.Output = tailRunes(activity.Output, subagentUpdateLimit*2)
		activity.Latest = activity.Output
	case channel.EventPartAppended:
		if event.Part != nil {
			if text, ok := (*event.Part).AsText(); ok && strings.TrimSpace(text.Text) != "" {
				activity.Output = text.Text
				activity.Latest = text.Text
			}
		}
	case channel.EventToolLifecycle:
		if childTool != nil {
			activity.Latest = "tool " + childTool.ToolName + " " + string(childTool.Status)
			if childTool.ErrorMessage != "" {
				activity.Latest += ": " + childTool.ErrorMessage
			}
		}
	case channel.EventMessageFinal:
		activity.Phase = "completed"
		if event.Message != nil {
			if text := messageText(*event.Message); strings.TrimSpace(text) != "" {
				activity.Latest = text
			}
			if raw := event.Message.Meta["error"]; len(raw) > 0 {
				activity.Phase = "failed"
				var reason string
				if json.Unmarshal(raw, &reason) == nil && reason != "" {
					activity.Latest = reason
				}
			}
		}
	case channel.EventError:
		activity.Phase = "failed"
		activity.Latest = "child turn error"
	}
	m.subagents[callID] = activity
	m.upsertToolishRow(callID, m.renderSubagentActivity(activity), true)
}

func (m model) findSubagentActivity(taskID, threadID string) (string, subagentActivity) {
	var selected subagentActivity
	for callID, activity := range m.subagents {
		if threadID != "" && activity.ThreadID == threadID {
			return callID, activity
		}
		if activity.TaskID != taskID || activity.Phase != "working" || activity.ThreadID != "" {
			continue
		}
		if selected.CallID == "" || activity.StartedMS > selected.StartedMS {
			selected = activity
		}
	}
	return selected.CallID, selected
}

func (m model) renderSubagentActivity(activity subagentActivity) string {
	name := strings.TrimSpace(activity.Name)
	if name != "" && name != "subagent" {
		name = " " + name
	} else {
		name = ""
	}
	var label string
	switch activity.Phase {
	case "completed":
		label = "Subagent" + name + " completed"
	case "failed":
		label = "Subagent" + name + " failed"
	case "cancelled":
		label = "Subagent" + name + " cancelled"
	default:
		label = subagentWorkingFrames[m.thinkingFrame%len(subagentWorkingFrames)] + name
	}
	if latest := compactSubagentUpdate(activity.Latest); latest != "" {
		label += " · " + latest
	}
	if activity.ThreadID != "" && activity.Phase != "working" {
		label += " (" + shortID(activity.ThreadID) + ")"
	}
	return label
}

func compactSubagentUpdate(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if utf8.RuneCountInString(value) <= subagentUpdateLimit {
		return value
	}
	return "…" + tailRunes(value, subagentUpdateLimit-1)
}

func tailRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[len(runes)-limit:])
}
