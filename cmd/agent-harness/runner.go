package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func buildRunner(cfg appConfig, pub channel.Publisher, approvals approval.Awaiter, decorator func(context.Context, protocol.MessageAddress) context.Context) (*turn.Runner, *store.FileStore, *localtools.Workspace, error) {
	if err := os.MkdirAll(cfg.workspaceRoot, 0o755); err != nil {
		return nil, nil, nil, err
	}
	if cfg.seed {
		seedDefaults(cfg.workspaceRoot)
	}

	ws, err := localtools.NewWorkspace(cfg.workspaceRoot)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("workspace: %w", err)
	}
	exa, err := localtools.NewExaClient("https://api.exa.ai/search", os.Getenv("EXA_API_KEY"), true, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("exa: %w", err)
	}
	reg := tools.NewRegistry()
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws, Python: "python3", Exa: exa, Timeout: 30 * time.Second, MaxOutput: 1 << 20}); err != nil {
		return nil, nil, nil, fmt.Errorf("register tools: %w", err)
	}
	subagentRef := &subagent.Ref{}
	agentCatalog, err := agentCatalogForWorkspace(cfg.workspaceRoot)
	if err != nil {
		return nil, nil, nil, err
	}
	// A publisher that can also enqueue inbound turns (web/tui channels) lets a
	// detached background sub-agent push its completion back to the parent thread;
	// cli (stdout-only) cannot, so background stays disabled there.
	notifier, allowBackground := pub.(channel.Enqueuer)
	if err := registerReferenceSubagent(reg, subagentRef, agentCatalog, allowBackground); err != nil {
		return nil, nil, nil, fmt.Errorf("register subagent: %w", err)
	}

	auth := ""
	if k := os.Getenv("SAHARA_LLM_API_KEY"); k != "" {
		auth = "Bearer " + k
	}
	// StreamUsage: the oss CLI talks to a standard OpenAI-compatible endpoint that
	// sends a terminal `[DONE]`/usage chunk, so request streamed token usage for
	// trace/export. (Distributions behind a proxy that omits `[DONE]` leave it off.)
	prov, err := provider.NewOpenAICompatClient(provider.OpenAICompatConfig{BaseURL: cfg.baseURL, AuthHeader: auth, Format: provider.FormatOpenAI, StreamUsage: true})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("provider: %w", err)
	}

	fsStore := store.NewFileStore(cfg.workspaceRoot, cfg.stateDir)
	skillSpecs, skillWarnings, _ := skills.DiscoverWithWarnings(cfg.workspaceRoot, []string{"skills", ".agent-harness-skills"})
	// Register load_skill so the model loads a skill by name — resolving its file
	// and any prerequisite skills — instead of read_file'ing the path. The same
	// mechanism the sahara distribution uses; nothing about it is distribution-specific.
	skillReg := skills.BuildRegistry(skillSpecs, cfg.workspaceRoot)
	if err := reg.Register(skills.LoadToolName, skills.LoadTool(skillReg)); err != nil {
		return nil, nil, nil, fmt.Errorf("register load_skill: %w", err)
	}
	cmdSpecs, _ := commands.Discover(cfg.workspaceRoot, []string{"commands", ".agent-harness-commands"})

	// Pluggable compaction: default to the evict→classic composite (evicts oversized
	// tool results/text to a workspace file the model can read_file, then classic
	// drop-oldest if still over budget). AGENT_HARNESS_COMPACTION overrides the mode
	// ("classic" keeps the historical behavior; "summarize" is opt-in but needs no
	// Summarizer wired here yet, so it degrades to classic).
	compactor := compaction.CompactorFor(os.Getenv("AGENT_HARNESS_COMPACTION"), localtools.NewEvictionSink(ws, ""), nil)

	// Opt-in trace/training recorder: off unless --record <path> is set.
	var recorder turn.TurnRecorder
	if cfg.record != "" {
		fr, rerr := newFileTurnRecorder(cfg.record)
		if rerr != nil {
			return nil, nil, nil, fmt.Errorf("record: %w", rerr)
		}
		recorder = fr
	}

	runner, err := turn.NewRunner(turn.Config{
		Store:     fsStore,
		Loader:    fsStore,
		Registry:  reg,
		Provider:  prov,
		Publisher: pub,
		Assembler: contextpack.NewAssembler(contextpack.Config{
			MaxHistoryMessages: 24,
			MaxTextPartBytes:   64 << 10,
			MaxFileBytes:       16 << 10,
			Compactor:          compactor,
			Skills:             skillSummaries(skillSpecs),
			SkillLoadTool:      true,
			SkillBodies:        skillReg.Body,
			SkillLoadWarnings:  skillWarnings,
			WorkspaceDir:       cfg.workspaceRoot,
			Model:              cfg.model,
			Now:                time.Now,
		}),
		Model:     cfg.model,
		Streaming: true,
		// Bound the token-delta message rate on transports that pay per message
		// (Aether); 0 on the in-process channels streams every delta.
		StreamFlushInterval: cfg.streamFlush,
		TurnRecorder:        recorder,
		Commands:            commands.New(cmdSpecs),
		Now:                 time.Now,
		Approvals:           approvals,
		// Notifier wakes a fresh parent turn with a background sub-agent's completion
		// notice; nil (cli) → background spawns fall back to synchronous.
		Notifier: notifier,
		// Interactive web/TUI channels can use child-thread stream events to keep the
		// blocking spawn_subagent row live with the child's latest activity.
		StreamSubagents: allowBackground,
		// Detached children use the same child-thread event stream.
		StreamBackgroundSubagents: allowBackground,
		// ContextDecorator wraps each turn's ctx (ACP passes ac.TurnContext to route
		// file/shell tools through the client; other channels pass nil → unchanged).
		ContextDecorator: decorator,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("runner: %w", err)
	}
	subagentRef.Set(runner)
	return runner, fsStore, ws, nil
}
