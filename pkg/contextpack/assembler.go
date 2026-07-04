package contextpack

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

type Config struct {
	MaxHistoryMessages int
	MaxTextPartBytes   int
	// MaxContextTokens, when > 0, caps the estimated tokens of the assembled
	// context (system prompt + history). History is trimmed to fit the budget
	// remaining after the system prompt. 0 disables token budgeting (count/byte
	// caps still apply, plus the turn loop's reactive overflow recovery).
	MaxContextTokens int

	// InvokedSkillTTL bounds how many turns a load_skill invocation stays listed
	// in the "## Loaded skills" prompt section. 0 -> defaultInvokedSkillTTL.
	InvokedSkillTTL int

	// SkillRelevance, when set, ranks/selects the skills listed each turn and may
	// nominate skills to auto-realize (inject their body). nil -> the full Skills
	// catalog is listed in the cacheable prefix and nothing is auto-realized.
	SkillRelevance SkillRelevanceProvider
	// SkillBodies resolves a skill's body for auto-realization. nil -> auto-realize
	// is disabled even when a provider nominates skills. Typically the skill registry.
	SkillBodies SkillBodyResolver
	// AutoRealizeStaleTurns: a nominated skill already loaded via load_skill within
	// this many turns is considered still in-context and is NOT re-injected (dedup).
	// 0 -> defaultAutoRealizeStaleTurns.
	AutoRealizeStaleTurns int
	// MaxAutoRealizeBytes caps the total body bytes injected by auto-realize per
	// turn. 0 -> defaultMaxAutoRealizeBytes.
	MaxAutoRealizeBytes int

	// System-prompt inputs (see internal/sysprompt).
	Base         string
	Tools        []sysprompt.ToolSummary
	Subagents    bool // spawn_subagent available → emit delegation guidance
	Skills       []sysprompt.SkillSummary
	MemoryTools  bool
	MaxFileBytes int
	WorkspaceDir string
	Model        string
	SandboxID    string
	Now          func() time.Time
}

// defaultInvokedSkillTTL: past this many turns since it was loaded, a skill drops
// off the "## Loaded skills" list (its body is long gone from context, and the
// catalog still lets the model reload it). Generous: the ledger is bounded by
// distinct skills, so this is a relevance cutoff, not a memory bound.
const defaultInvokedSkillTTL = 100

// defaultAutoRealizeStaleTurns: a skill loaded via load_skill within this many
// turns is treated as still-in-context, so auto-realize won't duplicate its body.
const defaultAutoRealizeStaleTurns = 2

// defaultMaxAutoRealizeBytes caps the total body bytes auto-realize injects per
// turn, so a permissive provider can't blow the context budget.
const defaultMaxAutoRealizeBytes = 32 << 10

type Assembler struct {
	cfg Config
}

func NewAssembler(cfg Config) Assembler {
	return Assembler{cfg: cfg}
}

// workingState extracts the compaction WorldState from history (which survives
// compaction) and returns the still-relevant, aged working state — invoked
// skills, recent files, and todos — for the dynamic prompt sections. Age is
// measured against the in-flight turn (carried on ctx), not the last turn already
// stamped in history (which is one behind). Each list is TTL-bounded.
func (a Assembler) workingState(ctx context.Context, history []protocol.ChatMessage) ([]sysprompt.LoadedSkill, []sysprompt.RecentFile, []sysprompt.TodoLine, []sysprompt.SubagentLine) {
	ttl := a.cfg.InvokedSkillTTL
	if ttl <= 0 {
		ttl = defaultInvokedSkillTTL
	}
	ws := compaction.ExtractWorldState(history)
	if n, ok := compaction.TurnNumberFrom(ctx); ok && n > ws.Turn {
		ws.Turn = n
	}
	var skills []sysprompt.LoadedSkill
	for _, s := range ws.ActiveInvokedSkills(ttl) {
		skills = append(skills, sysprompt.LoadedSkill{Name: s.Name, AgeTurns: ws.Turn - s.LastTurn})
	}
	var files []sysprompt.RecentFile
	for _, f := range ws.ActiveRecentFiles(ttl) {
		files = append(files, sysprompt.RecentFile{Path: f.Path, Kind: f.Kind, AgeTurns: ws.Turn - f.LastTurn})
	}
	var todos []sysprompt.TodoLine
	for _, t := range ws.ActiveTodos(ttl) {
		todos = append(todos, sysprompt.TodoLine{Content: t.Content, Status: t.Status, AgeTurns: ws.Turn - t.LastTurn})
	}
	var subagents []sysprompt.SubagentLine
	for _, h := range ws.ActiveSubagentHandles(ttl) {
		subagents = append(subagents, sysprompt.SubagentLine{
			Name: h.Name, ThreadID: h.ID, Status: h.Status, Summary: h.Summary, AgeTurns: ws.Turn - h.LastTurn,
		})
	}
	return skills, files, todos, subagents
}

// applyRelevance runs the optional SkillRelevanceProvider and returns the skills
// to list, whether that listing is per-turn (so sysprompt renders it in the
// dynamic suffix), and any skill bodies to auto-realize this turn. With no
// provider — or on a provider error — it returns the full catalog unchanged, not
// dynamic, and nothing auto-loaded (today's behavior).
func (a Assembler) applyRelevance(ctx context.Context, history []protocol.ChatMessage, realized []sysprompt.LoadedSkill) ([]sysprompt.SkillSummary, bool, []sysprompt.AutoLoadedSkill) {
	if a.cfg.SkillRelevance == nil || len(a.cfg.Skills) == 0 {
		return a.cfg.Skills, false, nil
	}
	turn := 0
	if n, ok := compaction.TurnNumberFrom(ctx); ok {
		turn = n
	}
	res, err := a.cfg.SkillRelevance.RankSkills(ctx, SkillRelevanceRequest{
		Available: candidatesFrom(a.cfg.Skills),
		History:   history,
		Realized:  realizedFrom(realized),
		Turn:      turn,
	})
	if err != nil {
		return a.cfg.Skills, false, nil
	}
	skills, dynamic := a.rankedListing(res)
	return skills, dynamic, a.autoRealize(res.Realize, realized)
}

// rankedListing turns a provider result into the skills to list. An empty ranking
// leaves the full catalog in the cacheable prefix; otherwise the listing becomes
// per-turn (dynamic) — refined (ranked first, then the rest) or replaced (ranked
// only). Unknown ranked names are dropped.
func (a Assembler) rankedListing(res SkillRelevanceResult) ([]sysprompt.SkillSummary, bool) {
	if len(res.Listed) == 0 {
		return a.cfg.Skills, false
	}
	byName := make(map[string]sysprompt.SkillSummary, len(a.cfg.Skills))
	for _, s := range a.cfg.Skills {
		byName[s.Name] = s
	}
	ranked := make(map[string]bool, len(res.Listed))
	out := make([]sysprompt.SkillSummary, 0, len(a.cfg.Skills))
	for _, r := range res.Listed {
		s, ok := byName[r.Name]
		if !ok || ranked[r.Name] {
			continue
		}
		ranked[r.Name] = true
		s.Note = r.Note
		out = append(out, s)
	}
	if res.Mode == ListRefine {
		for _, s := range a.cfg.Skills { // append the unranked catalog, original order
			if !ranked[s.Name] {
				out = append(out, s)
			}
		}
	}
	return out, true
}

// autoRealize resolves the bodies of nominated skills to inject this turn. It
// dedups against skills loaded via load_skill within AutoRealizeStaleTurns (their
// body is still in context) and against repeats, skips unresolvable ones, and caps
// the total injected bytes. nil resolver disables it entirely.
func (a Assembler) autoRealize(names []string, realized []sysprompt.LoadedSkill) []sysprompt.AutoLoadedSkill {
	if a.cfg.SkillBodies == nil || len(names) == 0 {
		return nil
	}
	stale := a.cfg.AutoRealizeStaleTurns
	if stale <= 0 {
		stale = defaultAutoRealizeStaleTurns
	}
	fresh := make(map[string]bool, len(realized))
	for _, r := range realized {
		if r.AgeTurns <= stale {
			fresh[r.Name] = true
		}
	}
	maxBytes := a.cfg.MaxAutoRealizeBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxAutoRealizeBytes
	}
	seen := make(map[string]bool, len(names))
	var out []sysprompt.AutoLoadedSkill
	total := 0
	for _, name := range names {
		if name == "" || seen[name] || fresh[name] {
			continue
		}
		seen[name] = true
		body, ok := a.cfg.SkillBodies(name)
		if !ok || body == "" {
			continue
		}
		if total+len(body) > maxBytes {
			continue // skip this one; a later smaller skill may still fit
		}
		total += len(body)
		out = append(out, sysprompt.AutoLoadedSkill{Name: name, Body: body})
	}
	return out
}

func candidatesFrom(skills []sysprompt.SkillSummary) []SkillCandidate {
	out := make([]SkillCandidate, len(skills))
	for i, s := range skills {
		out[i] = SkillCandidate{Name: s.Name, Description: s.Description}
	}
	return out
}

func realizedFrom(loaded []sysprompt.LoadedSkill) []RealizedSkill {
	out := make([]RealizedSkill, len(loaded))
	for i, s := range loaded {
		out[i] = RealizedSkill{Name: s.Name, AgeTurns: s.AgeTurns}
	}
	return out
}

func (a Assembler) Build(ctx context.Context, bootstrap []bootstrap.File, history []protocol.ChatMessage) ([]protocol.ChatMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	messages := make([]protocol.ChatMessage, 0, len(history)+1)

	var now time.Time
	if a.cfg.Now != nil {
		now = a.cfg.Now()
	}
	wsSkills, wsFiles, wsTodos, wsSubagents := a.workingState(ctx, history)
	// Relevance seam: a provider may reorder/replace the listed skills and inject
	// relevant skill bodies. Default (nil provider) keeps the full catalog in the
	// cacheable prefix and auto-realizes nothing — identical to before.
	promptSkills, skillsDynamic, autoLoaded := a.applyRelevance(ctx, history, wsSkills)
	sysMsg, err := sysprompt.Build(sysprompt.Input{
		Base:             a.cfg.Base,
		Bootstrap:        bootstrap,
		Tools:            a.cfg.Tools,
		Skills:           promptSkills,
		SkillsDynamic:    skillsDynamic,
		AutoLoadedSkills: autoLoaded,
		LoadedSkills:     wsSkills,
		RecentFiles:      wsFiles,
		Todos:            wsTodos,
		Subagents:        wsSubagents,
		MemoryTools:      a.cfg.MemoryTools,
		SubagentsEnabled: a.cfg.Subagents,
		MaxFileBytes:     a.cfg.MaxFileBytes,
		WorkspaceDir:     a.cfg.WorkspaceDir,
		Model:            a.cfg.Model,
		SandboxID:        a.cfg.SandboxID,
		Now:              now,
	}).Message()
	if err != nil {
		return nil, fmt.Errorf("build system prompt: %w", err)
	}
	messages = append(messages, sysMsg)

	// The context ceiling is the model the runner is about to call: a per-call budget
	// on ctx (the current/escalated model's window) overrides the static config, so an
	// escalation to a smaller-context model compacts history to fit it. Falls back to
	// the static MaxContextTokens when no per-call budget is set.
	maxContextTokens := a.cfg.MaxContextTokens
	if b, ok := compaction.ContextBudgetFrom(ctx); ok && b > 0 {
		maxContextTokens = b
	}
	historyTokenBudget := 0
	if maxContextTokens > 0 {
		historyTokenBudget = maxContextTokens - compaction.EstimateTokens([]protocol.ChatMessage{sysMsg})
		if historyTokenBudget < 1 {
			historyTokenBudget = 1 // keep at least the most recent message
		}
	}
	report, err := compaction.ReduceWithReport(history, compaction.Config{
		MaxMessages:      a.cfg.MaxHistoryMessages,
		MaxTextPartBytes: a.cfg.MaxTextPartBytes,
		MaxTokens:        historyTokenBudget,
	})
	if err != nil {
		return nil, fmt.Errorf("compact context: %w", err)
	}
	// Record a compaction EVENT when this Build actually dropped messages, so the
	// per-thread WorldState.Compactions counter advances (record-keeping only).
	if report.Budget.CompactedMessages > 0 {
		compaction.NoteCompaction(ctx)
	}
	messages = append(messages, report.Messages...)
	return messages, nil
}
