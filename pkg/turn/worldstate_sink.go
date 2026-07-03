package turn

import (
	"context"
	"encoding/json"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// turnWorldStateSink is the per-turn tools.WorldStateSink. It collects durable
// world-state contributions from tools (currently invoked skills) and produces a
// compaction.WorldState fragment stamped with this turn's counter, which the
// runner writes onto the turn's assistant message meta so it survives compaction
// via ExtractWorldState.
type turnWorldStateSink struct {
	turn      int
	mu        sync.Mutex
	skills    map[string]compaction.SkillRef
	files     map[string]compaction.FileMetadata
	todos     map[string]compaction.TodoState
	subagents map[string]compaction.SubagentHandle
}

func newTurnWorldStateSink(turn int) *turnWorldStateSink {
	return &turnWorldStateSink{
		turn:      turn,
		skills:    map[string]compaction.SkillRef{},
		files:     map[string]compaction.FileMetadata{},
		todos:     map[string]compaction.TodoState{},
		subagents: map[string]compaction.SubagentHandle{},
	}
}

func (s *turnWorldStateSink) RecordInvokedSkill(name, source string) {
	if name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.skills[name] = compaction.SkillRef{Name: name, Source: source, LastTurn: s.turn}
}

func (s *turnWorldStateSink) RecordFile(path, kind string) {
	if path == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = compaction.FileMetadata{Path: path, Kind: kind, LastTurn: s.turn}
}

func (s *turnWorldStateSink) RecordTodos(items []spec.TodoItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range items {
		key := it.ID
		if key == "" {
			key = it.Content // fall back to content when the model omits an id
		}
		if key == "" {
			continue
		}
		s.todos[key] = compaction.TodoState{
			ID: key, Content: it.Content, Status: string(it.Status), LastTurn: s.turn,
		}
	}
}

func (s *turnWorldStateSink) RecordSubagent(id, name, status, summary string) {
	if id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subagents[id] = compaction.SubagentHandle{
		ID: id, Name: name, Status: status, Summary: summary, LastTurn: s.turn,
	}
}

// worldState returns the fragment for this turn: the advanced turn counter plus
// any state recorded so far. Always carries Turn (so age advances every turn,
// even ones that record nothing).
func (s *turnWorldStateSink) worldState() compaction.WorldState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := compaction.WorldState{Turn: s.turn}
	for _, sk := range s.skills {
		out.InvokedSkills = append(out.InvokedSkills, sk)
	}
	for _, f := range s.files {
		out.RecentFiles = append(out.RecentFiles, f)
	}
	for _, t := range s.todos {
		out.Todos = append(out.Todos, t)
	}
	for _, sa := range s.subagents {
		out.ActiveSubagents = append(out.ActiveSubagents, sa)
	}
	return out
}

// recordToolFiles records any files a tool wrote/edited (from result.Metadata.
// FileChanges) onto the per-turn world-state sink, so RecentFiles ages + survives
// compaction. No-op when no sink is wired.
func recordToolFiles(ctx context.Context, result tools.Result) {
	if len(result.Metadata.FileChanges) == 0 {
		return
	}
	sink, ok := tools.WorldStateSinkFrom(ctx)
	if !ok {
		return
	}
	for _, fc := range result.Metadata.FileChanges {
		sink.RecordFile(fc.Path, fc.Kind)
	}
}

// stampTurnWorldState writes the current turn's world-state fragment onto msg.Meta
// so it persists in history (and rides ExtractWorldState across compaction). No-op
// when no sink is wired. Applied to each assistant message as it's appended; the
// final assistant of the turn carries the complete skill set (tools run before it).
func stampTurnWorldState(ctx context.Context, msg *protocol.ChatMessage) {
	sink, ok := tools.WorldStateSinkFrom(ctx)
	if !ok {
		return
	}
	ts, ok := sink.(*turnWorldStateSink)
	if !ok {
		return
	}
	raw, err := json.Marshal(ts.worldState())
	if err != nil {
		return
	}
	if msg.Meta == nil {
		msg.Meta = map[string]json.RawMessage{}
	}
	msg.Meta[compaction.MetaWorldState] = raw
}
