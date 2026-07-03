package tools

import (
	"context"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// WorldStateSink lets a tool record durable, compaction-surviving world-state
// contributions — e.g. load_skill recording which skill the model loaded this
// turn. The runner provides one per turn via ctx and folds the collected state
// into the turn's message meta, so it rides ExtractWorldState across compaction
// (unlike the tool_result body, which is a droppable message). Like PartEmitter
// and MemoryAuthority, it is a request-scoped capability carried on ctx rather
// than threaded through every tool signature.
type WorldStateSink interface {
	// RecordInvokedSkill notes that the model loaded a skill this turn (its name
	// and origin). The sink stamps the current turn as the skill's LastTurn.
	RecordInvokedSkill(name, source string)
	// RecordFile notes a file the agent wrote or edited this turn (kind is
	// "write" or "edit"). Aged in WorldState.RecentFiles.
	RecordFile(path, kind string)
	// RecordTodos records the current todo board (the model rewrites the whole
	// list via todo_write). Items are merged by id, keeping the latest touch, so
	// the board survives compaction.
	RecordTodos(items []spec.TodoItem)
	// RecordSubagent notes a sub-agent the model spawned/resumed this turn: its
	// thread_id handle (id, the re-addressable key), display name, status, and
	// short summary. Aged in WorldState.ActiveSubagents so the orchestrator sees
	// its live specialists and can route follow-ups back to them. Primitives only
	// (no compaction import from this package).
	RecordSubagent(id, name, status, summary string)
}

type worldStateSinkKey struct{}

// WithWorldStateSink returns a context carrying the per-turn WorldStateSink.
func WithWorldStateSink(ctx context.Context, s WorldStateSink) context.Context {
	return context.WithValue(ctx, worldStateSinkKey{}, s)
}

// WorldStateSinkFrom returns the WorldStateSink carried on ctx, and whether one
// was present. Absence is a no-op (e.g. a transport with no world-state plumbing).
func WorldStateSinkFrom(ctx context.Context) (WorldStateSink, bool) {
	s, ok := ctx.Value(worldStateSinkKey{}).(WorldStateSink)
	return s, ok
}
