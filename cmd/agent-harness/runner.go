package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/refinement"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func buildRunner(cfg appConfig, st stores, pub channel.Publisher, approvals approval.Awaiter, subagentTasks subagent.TaskBackend, notifier channel.Enqueuer, continuationBackend goal.ContinuationBackend, authorityHandoff *authhandoff.Store, decorator func(context.Context, protocol.MessageAddress) context.Context, scheduledOperations turn.ScheduledOperationsCommandProvider) (*turn.Runner, *localtools.Workspace, error) {
	if st.subagents != nil {
		var err error
		if taskRecovery, ok := st.subagents.(subagent.TaskRecoveryRegistry); ok && subagentTasks != nil {
			err = taskRecovery.RecoverInterruptedWithTasks(context.Background(), time.Now(), subagentTasks)
		} else {
			err = st.subagents.RecoverInterrupted(context.Background(), time.Now())
		}
		if err != nil {
			return nil, nil, fmt.Errorf("subagents: recover interrupted lifecycle: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.workspaceRoot, 0o755); err != nil {
		return nil, nil, err
	}
	if cfg.seed {
		seedDefaults(cfg.workspaceRoot)
	}

	ws, err := localtools.NewWorkspace(cfg.workspaceRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("workspace: %w", err)
	}
	exa, err := localtools.NewExaClient("https://api.exa.ai/search", os.Getenv("EXA_API_KEY"), true, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("exa: %w", err)
	}
	reg := tools.NewRegistry()
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws, Python: "python3", Exa: exa, Timeout: 30 * time.Second, MaxOutput: 1 << 20}); err != nil {
		return nil, nil, fmt.Errorf("register tools: %w", err)
	}
	subagentRef := &subagent.Ref{}
	// A publisher that can also enqueue inbound turns (web/tui channels) lets a
	// detached background sub-agent push its completion back to the parent thread;
	// cli (stdout-only) cannot, so background stays disabled there.
	if notifier == nil {
		notifier, _ = pub.(channel.Enqueuer)
	}
	allowBackground := notifier != nil
	if authorityHandoff == nil {
		authorityHandoff = authhandoff.New()
	}
	turnOwnerIdentity := cfg.sourceAgent()
	if topic, ok := notifier.(interface{ Topic() string }); ok && topic.Topic() != "" {
		turnOwnerIdentity = topic.Topic()
	}
	if err := registerReferenceSubagent(reg, subagentRef, st.agentCatalog, allowBackground); err != nil {
		return nil, nil, fmt.Errorf("register subagent: %w", err)
	}
	if st.refinements != nil {
		if err := refinement.RegisterTools(reg, st.refinements); err != nil {
			return nil, nil, fmt.Errorf("register refinement tools: %w", err)
		}
	}

	var goalRuntime *goal.Runtime
	if st.goals != nil || st.continuations != nil {
		if st.goals == nil || st.continuations == nil {
			return nil, nil, errors.New("goal runtime requires both lifecycle store and continuation ledger")
		}
		goalService, err := goal.NewService(goal.ServiceConfig{Store: st.goals, Now: time.Now})
		if err != nil {
			return nil, nil, fmt.Errorf("goal service: %w", err)
		}
		if err := goal.RegisterTools(reg, goalService); err != nil {
			return nil, nil, fmt.Errorf("register goal tools: %w", err)
		}
		goalRuntime, err = goal.NewRuntime(goal.RuntimeConfig{
			Service: goalService, Ledger: st.continuations,
			Policy:              goal.BoundedPolicy{MaxContinuations: cfg.goalMaxContinuations},
			ContinuationBackend: continuationBackend,
			Enqueuer:            notifier, AuthHandoff: authorityHandoff,
			DefaultWorkspaceID: effectiveWorkspace("", cfg.workspaceID),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("goal runtime: %w", err)
		}
	}

	auth := ""
	if k := os.Getenv("SAHARA_LLM_API_KEY"); k != "" {
		auth = "Bearer " + k
	}
	// StreamUsage: the oss CLI talks to a standard OpenAI-compatible endpoint that
	// sends a terminal `[DONE]`/usage chunk, so request streamed token usage for
	// trace/export. (Distributions behind a proxy that omits `[DONE]` leave it off.)
	wireFormat := provider.FormatOpenAI
	if cfg.llmFormat == "native" {
		wireFormat = provider.FormatNative
	}
	prov, err := provider.NewOpenAICompatClient(provider.OpenAICompatConfig{BaseURL: cfg.baseURL, AuthHeader: auth, Format: wireFormat, StreamUsage: true})
	if err != nil {
		return nil, nil, fmt.Errorf("provider: %w", err)
	}

	skillSpecs, skillWarnings, _ := skills.DiscoverWithWarnings(cfg.workspaceRoot, []string{"skills", ".agent-harness-skills"})
	// Register load_skill so the model loads a skill by name — resolving its file
	// and any prerequisite skills — instead of read_file'ing the path. The same
	// mechanism the sahara distribution uses; nothing about it is distribution-specific.
	skillReg := skills.BuildRegistry(skillSpecs, cfg.workspaceRoot)
	if err := reg.Register(skills.LoadToolName, skills.LoadTool(skillReg)); err != nil {
		return nil, nil, fmt.Errorf("register load_skill: %w", err)
	}
	reg.Describe(skills.LoadDescriptor())
	cmdSpecs, _ := commands.Discover(cfg.workspaceRoot, []string{"commands", ".agent-harness-commands"})

	// MemoryLayer catalogs are resolved only after the turn's logical workspace
	// has been defaulted. Successful skill registries and MCP managers are cached
	// independently per workspace; a cold remote failure falls back to this local
	// filesystem catalog for that turn and is retried later.
	var toolProviders []turn.ToolProvider
	if st.catalogs != nil {
		catalogRuntime := newWorkspaceCatalogRuntime(st.catalogs, cfg.workspaceRoot, skillSpecs, skillWarnings)
		decorator = composeContextDecorators(decorator, catalogRuntime.decorate)
		toolProviders = append(toolProviders, workspaceCatalogToolProvider{runtime: catalogRuntime})
	}

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
			return nil, nil, fmt.Errorf("record: %w", rerr)
		}
		recorder = fr
	}
	var turnObservers []hooks.TurnObserver
	if st.executionLedger != nil {
		turnObservers = append(turnObservers, st.executionLedger)
	}

	runner, err := turn.NewRunner(turn.Config{
		// Transcripts may live remotely (MemoryLayer); workspace bootstrap
		// documents are always local files.
		Store:     st.history,
		Loader:    st.files,
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
			PromptNotes:        st.promptNotes,
			WorkspaceDir:       cfg.workspaceRoot,
			Model:              cfg.model,
			Now:                time.Now,
		}),
		Model:         cfg.model,
		ModelRegistry: cfg.modelRegistry,
		ProviderResolver: turn.NewProviderResolver(
			cfg.modelRegistry,
			modelpkg.ProviderConfig{
				BaseURL: cfg.baseURL,
				APIKey:  os.Getenv("SAHARA_LLM_API_KEY"),
				Format:  cfg.llmFormat,
			},
			nil,
		),
		DefaultWorkspaceID: cfg.workspaceID,
		Streaming:          true,
		// Semantic recall from MemoryLayer, when configured. Auto-commit stays
		// off: the history store already writes the transcript to MemoryLayer,
		// and MemoryLayer distils memories from it on its own schedule.
		Memory:                   st.memory,
		MemoryAutoRecall:         st.memory != nil && cfg.memoryRecall,
		MemoryRecallLimit:        cfg.memoryRecallLimit,
		MemoryRecallIncludeInput: true,
		// Bound the token-delta message rate on transports that pay per message
		// (Aether); 0 on the in-process channels streams every delta.
		StreamFlushInterval: cfg.streamFlush,
		TurnRecorder:        recorder,
		Commands:            commands.New(cmdSpecs),
		ScheduledOperations: scheduledOperations,
		RefinementAudit:     st.refinements,
		ExecutionLedger:     st.executionLedger,
		TurnObservers:       turnObservers,
		Now:                 time.Now,
		Approvals:           approvals,
		ToolProviders:       toolProviders,
		// Notifier wakes a fresh parent turn with a background sub-agent's completion
		// notice; nil (cli) → background spawns fall back to synchronous.
		Notifier:                 notifier,
		AuthHandoff:              authorityHandoff,
		SubagentObserver:         st.subagents,
		SubagentDefaultWorkspace: effectiveWorkspace("", cfg.workspaceID),
		SubagentTasks:            subagentTasks,
		TurnJournal:              st.turns,
		TurnOwnerIdentity:        turnOwnerIdentity,
		Goals:                    goalRuntime,
		// Interactive web/TUI channels can use child-thread stream events to keep the
		// blocking spawn_subagent row live with the child's latest activity.
		StreamSubagents: allowBackground || subagentTasks != nil,
		// Detached children use the same child-thread event stream.
		StreamBackgroundSubagents: allowBackground,
		// ContextDecorator wraps each turn's ctx (ACP passes ac.TurnContext to route
		// file/shell tools through the client; other channels pass nil → unchanged).
		ContextDecorator: decorator,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("runner: %w", err)
	}
	subagentRef.Set(runner)
	return runner, ws, nil
}
