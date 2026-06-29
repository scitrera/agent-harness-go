package compaction

import (
	"encoding/json"
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const (
	MetaWorldState    = "scitrera_world_state"
	MetaContextBudget = "scitrera_context_budget"
)

type Report struct {
	Messages   []protocol.ChatMessage
	WorldState WorldState
	Budget     ContextBudget
}

type WorldState struct {
	PlanRefs        []PlanRef        `json:"plan_refs,omitempty"`
	Tasks           []TaskState      `json:"tasks,omitempty"`
	Todos           []TodoState      `json:"todos,omitempty"`
	InvokedSkills   []SkillRef       `json:"invoked_skills,omitempty"`
	RecentFiles     []FileMetadata   `json:"recent_files,omitempty"`
	DynamicTools    []string         `json:"dynamic_tools,omitempty"`
	ActiveSubagents []SubagentHandle `json:"active_subagents,omitempty"`
	Extra           json.RawMessage  `json:"extra,omitempty"`
}

type PlanRef struct {
	ID     string `json:"id"`
	Path   string `json:"path,omitempty"`
	Status string `json:"status,omitempty"`
}

type TaskState struct {
	ID        string   `json:"id"`
	Title     string   `json:"title,omitempty"`
	Status    string   `json:"status,omitempty"`
	BlockedBy []string `json:"blocked_by,omitempty"`
}

type TodoState struct {
	ID      string `json:"id"`
	Content string `json:"content,omitempty"`
	Status  string `json:"status,omitempty"`
}

type SkillRef struct {
	Name   string `json:"name"`
	Source string `json:"source,omitempty"`
}

type FileMetadata struct {
	Path   string `json:"path"`
	Digest string `json:"digest,omitempty"`
	Bytes  int64  `json:"bytes,omitempty"`
}

type SubagentHandle struct {
	ID     string `json:"id"`
	Task   string `json:"task,omitempty"`
	Status string `json:"status,omitempty"`
}

type ContextBudget struct {
	MaxTokens         int `json:"max_tokens,omitempty"`
	EstimatedTokens   int `json:"estimated_tokens"`
	RemainingTokens   int `json:"remaining_tokens"`
	MessageCount      int `json:"message_count"`
	CompactedMessages int `json:"compacted_messages,omitempty"`
}

func BudgetFor(messages []protocol.ChatMessage, maxTokens int, compactedMessages int) ContextBudget {
	estimated := EstimateTokens(messages)
	remaining := 0
	if maxTokens > 0 && estimated < maxTokens {
		remaining = maxTokens - estimated
	}
	return ContextBudget{
		MaxTokens:         maxTokens,
		EstimatedTokens:   estimated,
		RemainingTokens:   remaining,
		MessageCount:      len(messages),
		CompactedMessages: compactedMessages,
	}
}

func ExtractWorldState(messages []protocol.ChatMessage) WorldState {
	var out WorldState
	for _, msg := range messages {
		raw := msg.Meta[MetaWorldState]
		if len(raw) == 0 {
			continue
		}
		var state WorldState
		if err := json.Unmarshal(raw, &state); err != nil {
			continue
		}
		out = MergeWorldState(out, state)
	}
	return out
}

func MergeWorldState(states ...WorldState) WorldState {
	out := WorldState{}
	plans := map[string]PlanRef{}
	tasks := map[string]TaskState{}
	todos := map[string]TodoState{}
	skills := map[string]SkillRef{}
	files := map[string]FileMetadata{}
	tools := map[string]struct{}{}
	subagents := map[string]SubagentHandle{}
	for _, state := range states {
		for _, item := range state.PlanRefs {
			if item.ID != "" {
				plans[item.ID] = item
			}
		}
		for _, item := range state.Tasks {
			if item.ID != "" {
				tasks[item.ID] = normalizeTask(item)
			}
		}
		for _, item := range state.Todos {
			if item.ID != "" {
				todos[item.ID] = item
			}
		}
		for _, item := range state.InvokedSkills {
			if item.Name != "" {
				skills[item.Name] = item
			}
		}
		for _, item := range state.RecentFiles {
			if item.Path != "" {
				files[item.Path] = item
			}
		}
		for _, name := range state.DynamicTools {
			if name != "" {
				tools[name] = struct{}{}
			}
		}
		for _, item := range state.ActiveSubagents {
			if item.ID != "" {
				subagents[item.ID] = item
			}
		}
		if len(state.Extra) > 0 {
			out.Extra = append(json.RawMessage(nil), state.Extra...)
		}
	}
	out.PlanRefs = valuesByKey(plans)
	out.Tasks = valuesByKey(tasks)
	out.Todos = valuesByKey(todos)
	out.InvokedSkills = valuesByKey(skills)
	out.RecentFiles = valuesByKey(files)
	out.DynamicTools = stringKeys(tools)
	out.ActiveSubagents = valuesByKey(subagents)
	return out
}

func attachReportMetadata(messages []protocol.ChatMessage, state WorldState, budget ContextBudget) ([]protocol.ChatMessage, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	out := cloneMessages(messages)
	if out[0].Meta == nil {
		out[0].Meta = map[string]json.RawMessage{}
	}
	stateRaw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	budgetRaw, err := json.Marshal(budget)
	if err != nil {
		return nil, err
	}
	out[0].Meta[MetaWorldState] = stateRaw
	out[0].Meta[MetaContextBudget] = budgetRaw
	return out, nil
}

func normalizeTask(item TaskState) TaskState {
	out := item
	sort.Strings(out.BlockedBy)
	return out
}

func valuesByKey[T any](items map[string]T) []T {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]T, 0, len(keys))
	for _, key := range keys {
		out = append(out, items[key])
	}
	return out
}

func stringKeys(items map[string]struct{}) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
