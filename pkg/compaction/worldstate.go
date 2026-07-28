package compaction

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type turnNumberKey struct{}

// WithTurnNumber carries the current turn's number (the advanced WorldState.Turn
// for the in-flight turn) on ctx. The runner sets it once per turn so consumers —
// notably the context assembler computing invoked-skill age — measure age against
// "now", not against the last turn already stamped in history.
func WithTurnNumber(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, turnNumberKey{}, n)
}

// TurnNumberFrom returns the current turn number carried on ctx, if any.
func TurnNumberFrom(ctx context.Context) (int, bool) {
	n, ok := ctx.Value(turnNumberKey{}).(int)
	return n, ok
}

type contextBudgetKey struct{}

// WithContextBudget carries a per-call history token budget on ctx: the effective
// context ceiling for the model the runner is ABOUT to call. The runner sets it
// before each provider call (from the current/escalated model's window), so the
// assembler compacts history to fit the model actually in play — a turn that
// escalates a large-context orchestrator to a smaller-context vision model then
// trims to the vision model's window instead of overflowing it. A budget <= 0, or
// none set, leaves the assembler's static MaxContextTokens in force.
func WithContextBudget(ctx context.Context, tokens int) context.Context {
	return context.WithValue(ctx, contextBudgetKey{}, tokens)
}

// ContextBudgetFrom returns the per-call token budget carried on ctx, if any.
func ContextBudgetFrom(ctx context.Context) (int, bool) {
	n, ok := ctx.Value(contextBudgetKey{}).(int)
	return n, ok
}

type compactionCounterKey struct{}

// WithCompactionCounter returns ctx carrying a fresh per-turn compaction-event
// counter plus its pointer. The assembler bumps it (NoteCompaction) each time a
// context Build drops messages; the turn's world-state sink reads it to persist a
// running per-thread total (WorldState.Compactions).
func WithCompactionCounter(ctx context.Context) (context.Context, *int) {
	p := new(int)
	return context.WithValue(ctx, compactionCounterKey{}, p), p
}

// NoteCompaction bumps the ctx compaction-event counter (if present) by one. Called
// by the assembler when a Reduce actually dropped messages — counts EVENTS (per
// compacting provider call), not messages.
func NoteCompaction(ctx context.Context) {
	if p, ok := ctx.Value(compactionCounterKey{}).(*int); ok && p != nil {
		*p++
	}
}

// CompactionCount returns the current value of the ctx compaction-event counter
// (0 when absent). Lets the turn loop detect a compaction event by comparing the
// count across a context Build, without threading the counter pointer around.
func CompactionCount(ctx context.Context) int {
	if p, ok := ctx.Value(compactionCounterKey{}).(*int); ok && p != nil {
		return *p
	}
	return 0
}

const (
	MetaWorldState    = "scitrera_world_state"
	MetaContextBudget = "scitrera_context_budget"
	// MetaUsage carries the finalized assistant message's per-turn token
	// accounting ({model, prompt_tokens, completion_tokens, total_tokens, calls}),
	// summed across the turn's provider calls. Present only when a provider
	// reported usage; consumed by trace export.
	MetaUsage = "scitrera_usage"
)

type Report struct {
	Messages   []protocol.ChatMessage
	WorldState WorldState
	Budget     ContextBudget
}

type WorldState struct {
	// Turn is a monotonic per-thread turn counter that survives compaction (it
	// rides the WorldState meta). Used to age InvokedSkills: a skill's staleness
	// is Turn - SkillRef.LastTurn.
	Turn int `json:"turn,omitempty"`
	// Compactions is a monotonic per-thread count of context-compaction events (each
	// provider call whose context Build dropped messages). Survives compaction via the
	// WorldState meta. Record-keeping today (no behavioral effect); a future signal
	// source — e.g. a hint that older detail was compacted out and should be searched
	// in history/memory rather than assumed gone.
	Compactions     int              `json:"compactions,omitempty"`
	PlanRefs        []PlanRef        `json:"plan_refs,omitempty"`
	Tasks           []TaskState      `json:"tasks,omitempty"`
	Todos           []TodoState      `json:"todos,omitempty"`
	InvokedSkills   []SkillRef       `json:"invoked_skills,omitempty"`
	RecentFiles     []FileMetadata   `json:"recent_files,omitempty"`
	DynamicTools    []string         `json:"dynamic_tools,omitempty"`
	ActiveSubagents []SubagentHandle `json:"active_subagents,omitempty"`
	Extra           json.RawMessage  `json:"extra,omitempty"`
}

// LastTurn on the sub-structs below is the WorldState.Turn at which the entry was
// last touched, so consumers can age it (age = WorldState.Turn - LastTurn) and
// drop stale entries past a TTL — uniform with SkillRef. NOTE: InvokedSkills,
// RecentFiles, Todos, and ActiveSubagents have producers today; PlanRefs/Tasks/
// DynamicTools carry the field for uniformity but are not yet populated (no plan/
// task tools exist).

type PlanRef struct {
	ID       string `json:"id"`
	Path     string `json:"path,omitempty"`
	Status   string `json:"status,omitempty"`
	LastTurn int    `json:"last_turn,omitempty"`
}

type TaskState struct {
	ID        string   `json:"id"`
	Title     string   `json:"title,omitempty"`
	Status    string   `json:"status,omitempty"`
	BlockedBy []string `json:"blocked_by,omitempty"`
	LastTurn  int      `json:"last_turn,omitempty"`
}

type TodoState struct {
	ID       string `json:"id"`
	Content  string `json:"content,omitempty"`
	Status   string `json:"status,omitempty"`
	LastTurn int    `json:"last_turn,omitempty"`
}

type SkillRef struct {
	Name string `json:"name"`
	// Source is the skill's origin (e.g. path or catalog).
	Source string `json:"source,omitempty"`
	// LastTurn is the WorldState.Turn at which the skill was last invoked (via
	// load_skill). Age = current WorldState.Turn - LastTurn.
	LastTurn int `json:"last_turn,omitempty"`
}

type FileMetadata struct {
	Path     string `json:"path"`
	Kind     string `json:"kind,omitempty"` // write | edit
	Digest   string `json:"digest,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
	LastTurn int    `json:"last_turn,omitempty"`
}

type SubagentHandle struct {
	// ID is the child sub-agent's thread_id — the re-addressable handle returned
	// from spawn_subagent (pass it back as the `thread` arg to resume that child).
	// It is the dedup/merge key in MergeWorldState.
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Task     string `json:"task,omitempty"`
	Status   string `json:"status,omitempty"`
	Summary  string `json:"summary,omitempty"`
	LastTurn int    `json:"last_turn,omitempty"`
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
		if state.Turn > out.Turn {
			out.Turn = state.Turn
		}
		if state.Compactions > out.Compactions {
			out.Compactions = state.Compactions
		}
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
			if item.ID == "" {
				continue
			}
			if prev, ok := todos[item.ID]; !ok || item.LastTurn >= prev.LastTurn {
				todos[item.ID] = item
			}
		}
		for _, item := range state.InvokedSkills {
			if item.Name == "" {
				continue
			}
			// Dedup by name, keeping the most-recent invocation (max LastTurn).
			if prev, ok := skills[item.Name]; !ok || item.LastTurn >= prev.LastTurn {
				skills[item.Name] = item
			}
		}
		for _, item := range state.RecentFiles {
			if item.Path == "" {
				continue
			}
			if prev, ok := files[item.Path]; !ok || item.LastTurn >= prev.LastTurn {
				files[item.Path] = item
			}
		}
		for _, name := range state.DynamicTools {
			if name != "" {
				tools[name] = struct{}{}
			}
		}
		for _, item := range state.ActiveSubagents {
			if item.ID == "" {
				continue
			}
			// Dedup by thread_id, keeping the most-recent touch (max LastTurn) so a
			// resume updates the entry rather than duplicating it.
			if prev, ok := subagents[item.ID]; !ok || item.LastTurn >= prev.LastTurn {
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

// SkillAge returns turns since a skill was last invoked (Turn - LastTurn).
func (w WorldState) SkillAge(s SkillRef) int {
	age := w.Turn - s.LastTurn
	if age < 0 {
		age = 0
	}
	return age
}

// ActiveInvokedSkills returns the invoked skills whose age is <= ttl (ttl <= 0
// means no limit), ordered most-recently-invoked first. Used to surface the
// still-relevant loaded skills to the model without an unbounded, ever-growing
// list on long threads.
func (w WorldState) ActiveInvokedSkills(ttl int) []SkillRef {
	out := make([]SkillRef, 0, len(w.InvokedSkills))
	for _, s := range w.InvokedSkills {
		if ttl > 0 && w.SkillAge(s) > ttl {
			continue
		}
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LastTurn > out[j].LastTurn })
	return out
}

// ActiveRecentFiles returns files written/edited within ttl turns (ttl <= 0 =
// no limit), most-recent first.
func (w WorldState) ActiveRecentFiles(ttl int) []FileMetadata {
	out := make([]FileMetadata, 0, len(w.RecentFiles))
	for _, f := range w.RecentFiles {
		if ttl > 0 && w.Turn-f.LastTurn > ttl {
			continue
		}
		out = append(out, f)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LastTurn > out[j].LastTurn })
	return out
}

// ActiveTodos returns todos touched within ttl turns (ttl <= 0 = no limit),
// most-recently-updated first.
func (w WorldState) ActiveTodos(ttl int) []TodoState {
	out := make([]TodoState, 0, len(w.Todos))
	for _, t := range w.Todos {
		if ttl > 0 && w.Turn-t.LastTurn > ttl {
			continue
		}
		out = append(out, t)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LastTurn > out[j].LastTurn })
	return out
}

// ActiveSubagentHandles returns the sub-agents spawned this session whose age is
// <= ttl (ttl <= 0 = no limit), most-recently-touched first. Surfaces the live
// specialists (name + thread_id handle + status + summary) so the orchestrator can
// route a follow-up back to one instead of re-delegating.
func (w WorldState) ActiveSubagentHandles(ttl int) []SubagentHandle {
	out := make([]SubagentHandle, 0, len(w.ActiveSubagents))
	for _, h := range w.ActiveSubagents {
		if ttl > 0 && w.Turn-h.LastTurn > ttl {
			continue
		}
		out = append(out, h)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LastTurn > out[j].LastTurn })
	return out
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
