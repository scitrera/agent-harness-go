package main

import (
	"fmt"
	"os"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func buildRunner(cfg appConfig, pub channel.Publisher, approvals approval.Awaiter) (*turn.Runner, *store.FileStore, error) {
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
	agentCatalog, err := agentCatalogForWorkspace(cfg.workspaceRoot)
	if err != nil {
		return nil, nil, err
	}
	// A publisher that can also enqueue inbound turns (web/tui channels) lets a
	// detached background sub-agent push its completion back to the parent thread;
	// cli (stdout-only) cannot, so background stays disabled there.
	notifier, allowBackground := pub.(channel.Enqueuer)
	if err := registerReferenceSubagent(reg, subagentRef, agentCatalog, allowBackground); err != nil {
		return nil, nil, fmt.Errorf("register subagent: %w", err)
	}

	auth := ""
	if k := os.Getenv("SAHARA_LLM_API_KEY"); k != "" {
		auth = "Bearer " + k
	}
	prov, err := provider.NewSidecarClient(provider.SidecarConfig{BaseURL: cfg.baseURL, AuthHeader: auth, Format: provider.FormatOpenAI})
	if err != nil {
		return nil, nil, fmt.Errorf("provider: %w", err)
	}

	fsStore := store.NewFileStore(cfg.workspaceRoot, cfg.stateDir)
	skillSpecs, skillWarnings, _ := skills.DiscoverWithWarnings(cfg.workspaceRoot, []string{"skills", ".agent-harness-skills"})
	cmdSpecs, _ := commands.Discover(cfg.workspaceRoot, []string{"commands", ".agent-harness-commands"})

	// Pluggable compaction: default to the evict→classic composite (evicts oversized
	// tool results/text to a workspace file the model can read_file, then classic
	// drop-oldest if still over budget). AGENT_HARNESS_COMPACTION overrides the mode
	// ("classic" keeps the historical behavior; "summarize" is opt-in but needs no
	// Summarizer wired here yet, so it degrades to classic).
	compactor := compaction.CompactorFor(os.Getenv("AGENT_HARNESS_COMPACTION"), localtools.NewEvictionSink(ws, ""), nil)

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
			SkillLoadWarnings:  skillWarnings,
			WorkspaceDir:       cfg.workspaceRoot,
			Model:              cfg.model,
			Now:                time.Now,
		}),
		Model:     cfg.model,
		Streaming: true,
		Commands:  commands.New(cmdSpecs),
		Now:       time.Now,
		Approvals: approvals,
		// Notifier wakes a fresh parent turn with a background sub-agent's completion
		// notice; nil (cli) → background spawns fall back to synchronous.
		Notifier: notifier,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("runner: %w", err)
	}
	subagentRef.Set(runner)
	return runner, fsStore, nil
}
