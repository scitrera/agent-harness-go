// Command agent-harness is the reference CLI for the OSS agent-harness-go core.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/telemetry/otlpexport"
	"github.com/scitrera/agent-harness-go/pkg/version"
)

func main() {
	workspace := flag.String("workspace", env("SAHARA_WORKING_DIRECTORY", "./workspace"), "workspace root")
	workspaceMode := flag.String("workspace-mode", env("SAHARA_WORKSPACE_MODE", workspaceModeSingle), "workspace selection: single (legacy) or project (derive from cwd/Git)")
	workspaceID := flag.String("workspace-id", os.Getenv("SAHARA_WORKSPACE_ID"), "pin the logical workspace ID (enables composite history keys)")
	visibleWorkspaces := flag.String("visible-workspaces", os.Getenv("SAHARA_VISIBLE_WORKSPACES"), "comma-separated additional logical workspaces clients may address")
	workspaceIndexDir := flag.String("workspace-index-dir", env("SAHARA_WORKSPACE_INDEX_DIR", defaultWorkspaceIndexDir()), "shared project path-to-workspace index directory")
	thread := flag.String("thread", "cli", "chat thread id (CLI/TUI mode)")
	baseURL := flag.String("base-url", os.Getenv("SAHARA_LLM_BASE_URL"), "OpenAI-compatible base URL")
	model := flag.String("model", env("SAHARA_LLM_MODEL", "gpt-4o-mini"), "model id")
	llmFormat := flag.String("llm-format", env("SAHARA_LLM_FORMAT", "openai"), "provider request format: openai or native")
	seed := flag.Bool("seed", true, "seed default workspace files when missing")
	cliMode := flag.Bool("cli", false, "run the stdin REPL")
	tuiMode := flag.Bool("tui", false, "run the terminal UI (default)")
	acpMode := flag.Bool("acp", false, "run as an Agent Client Protocol (ACP) agent over stdio")
	webMode := flag.Bool("web", false, "run the localhost web UI")
	serveMode := flag.Bool("serve", false, "run as a headless agent worker reachable over Aether (requires --aether)")
	standalone := flag.Bool("aether-standalone", false, "run the agent worker and the terminal UI in one process, both over Aether (requires --aether)")
	aetherAddr := flag.String("aether", os.Getenv("AETHER_ADDR"), "Aether gateway address, e.g. 127.0.0.1:50051")
	aetherWorkspace := flag.String("aether-workspace", os.Getenv("AETHER_WORKSPACE"), "Aether workspace to join (defaults to the resolved logical workspace)")
	aetherSpecifier := flag.String("aether-specifier", os.Getenv("AETHER_SPECIFIER"), "Aether agent-topic specifier (distinguishes instances)")
	aetherUser := flag.String("aether-user", env("AETHER_USER", defaultAetherUser()), "client user id (Aether client modes)")
	aetherWindow := flag.String("aether-window", os.Getenv("AETHER_WINDOW"), "client window id; defaults to a fresh id per process")
	aetherTLS := flag.Bool("aether-tls", false, "use TLS for the Aether connection")
	aetherTLSInsecure := flag.Bool("aether-tls-insecure", false, "skip Aether TLS certificate verification (testing only)")
	aetherTaskMessageLanes := flag.Bool("aether-task-message-lanes", false, "route real Aether task turns over their subscribed per-task message lanes")
	subagentTarget := flag.String("subagent-target", os.Getenv("SAHARA_SUBAGENT_TARGET"), "execute spawned subagents on this full Aether agent topic (requires MemoryLayer)")
	subagentExecutor := flag.Bool("subagent-executor", false, "accept targeted agent-harness subagent tasks on this Aether worker (requires MemoryLayer)")
	subagentExecutorConcurrency := flag.Int("subagent-executor-concurrency", 4, "maximum concurrently assigned external subagents")
	memorylayerMode := flag.String("memorylayer-mode", env("MEMORYLAYER_MODE", memoryLayerModeAuto), "MemoryLayer routing: auto, off, http, or aether")
	memorylayerURL := flag.String("memorylayer", os.Getenv("MEMORYLAYER_BASE_URL"), "explicit MemoryLayer HTTP URL (overrides auto Aether discovery)")
	memorylayerTarget := flag.String("memorylayer-target", env("MEMORYLAYER_TARGET_TOPIC", defaultMemoryLayerTarget), "MemoryLayer Aether service topic")
	memorylayerWorkspace := flag.String("memorylayer-workspace", os.Getenv("MEMORYLAYER_WORKSPACE"), "MemoryLayer workspace (defaults to the resolved logical workspace)")
	promptNotesAuthority := flag.String("prompt-notes-authority", env("SAHARA_PROMPT_NOTES_AUTHORITY", promptNotesAuthorityLocal), "prompt-note authority: off, local, or memorylayer (no fallback or dual write)")
	agentSpecificationsAuthority := flag.String("agent-specifications-authority", env("SAHARA_AGENT_SPECIFICATIONS_AUTHORITY", agentSpecificationsAuthorityLocal), "agent-specification authority: off, local, or memorylayer (no fallback or dual write)")
	refinementAuthority := flag.String("refinement-authority", env("SAHARA_REFINEMENT_AUTHORITY", refinementAuthorityLocal), "refinement proposal/audit authority: off, local, or memorylayer (no fallback or dual write)")
	refinementSessionAutoApply := flag.Bool("refinement-session-auto-apply", strings.EqualFold(strings.TrimSpace(os.Getenv("SAHARA_REFINEMENT_SESSION_AUTO_APPLY")), "true"), "allow low-risk session-only refinements to apply without interactive approval")
	memoryRecall := flag.Bool("memory-recall", true, "inject MemoryLayer memories relevant to each message when available")
	memoryRecallLimit := flag.Int("memory-recall-limit", 5, "how many recalled memories to inject")
	goalMaxContinuations := flag.Uint("goal-max-continuations", 3, "maximum automatic follow-up turns per durable goal; 0 disables automatic continuation")
	addr := flag.String("addr", env("SAHARA_WEB_ADDR", "127.0.0.1:8787"), "web UI listen address (NO auth - localhost only)")
	noBrowser := flag.Bool("no-browser", false, "do not open a browser (web mode)")
	exportThread := flag.String("export", "", "export thread history as JSONL to stdout and exit (a thread id, or 'all')")
	exportFormat := flag.String("export-format", "openai", "export schema: openai (chat SFT) | trace (lossless spec messages)")
	record := flag.String("record", "", "record each LLM call (assembled prompt + response + usage) as JSONL to this path; off by default")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: agent-harness [flags]\n\nFlags:\n")
		flag.VisitAll(func(f *flag.Flag) {
			def := ""
			if f.DefValue != "" {
				def = fmt.Sprintf(" (default %q)", f.DefValue)
			}
			_, _ = fmt.Fprintf(flag.CommandLine.Output(), "  --%-12s %s%s\n", f.Name, f.Usage, def)
		})
	}
	flag.Parse()

	// Before every other mode check: a released binary must be able to report
	// what it is without a provider endpoint or a workspace.
	if *showVersion {
		fmt.Printf("agent-harness %s\n", version.String())
		return
	}
	stateDir := env("SAHARA_STATE_DIR", filepath.Join(*workspace, ".agent-harness"))
	workspaceResolution, err := resolveAppWorkspace(context.Background(), *workspaceMode, *workspaceID, *workspaceIndexDir, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: resolve workspace:", err)
		os.Exit(2)
	}

	// Export mode reads local state only (no provider needed): dump thread history
	// as JSONL and exit, before the base-url requirement below.
	if *exportThread != "" {
		if err := runWorkspaceExport(stateDir, workspaceResolution.WorkspaceID, *exportThread, *exportFormat, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	selectedMode, err := selectAppMode(*cliMode, *tuiMode, *acpMode, *webMode, *serveMode, *standalone)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if selectedMode.needsAether() && *aetherAddr == "" {
		fmt.Fprintf(os.Stderr, "error: --%s needs an Aether gateway: set --aether or AETHER_ADDR\n", selectedMode)
		os.Exit(2)
	}
	normalizedMemoryLayerMode, err := normalizeMemoryLayerMode(
		*memorylayerMode, *memorylayerURL, *memorylayerTarget, selectedMode.usesAether(*aetherAddr),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	hasMemoryLayer := memoryLayerConfigured(normalizedMemoryLayerMode)
	if err := validateExternalSubagentConfig(selectedMode, *subagentTarget, *subagentExecutor, hasMemoryLayer); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	normalizedPromptNotesAuthority, err := normalizePromptNotesAuthority(*promptNotesAuthority, hasMemoryLayer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	normalizedAgentSpecificationsAuthority, err := normalizeAgentSpecificationsAuthority(*agentSpecificationsAuthority, hasMemoryLayer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	normalizedRefinementAuthority, err := normalizeRefinementAuthority(*refinementAuthority, hasMemoryLayer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if *subagentExecutorConcurrency <= 0 {
		fmt.Fprintln(os.Stderr, "error: --subagent-executor-concurrency must be positive")
		os.Exit(2)
	}
	if *goalMaxContinuations > uint(^uint32(0)) {
		fmt.Fprintln(os.Stderr, "error: --goal-max-continuations is too large")
		os.Exit(2)
	}
	requiresMemoryLayer := normalizedMemoryLayerMode == memoryLayerModeAether ||
		strings.TrimSpace(*subagentTarget) != "" || *subagentExecutor ||
		normalizedPromptNotesAuthority == promptNotesAuthorityMemoryLayer ||
		normalizedAgentSpecificationsAuthority == agentSpecificationsAuthorityMemoryLayer ||
		normalizedRefinementAuthority == refinementAuthorityMemoryLayer
	// A pure client drives someone else's agent, so it needs no provider of its
	// own; every other mode runs turns locally and does.
	if *baseURL == "" && selectedMode.runsTurnsLocally(*aetherAddr) {
		fmt.Fprintln(os.Stderr, "error: set --base-url or SAHARA_LLM_BASE_URL (an OpenAI-compatible endpoint)")
		os.Exit(2)
	}
	var modelRegistry *modelpkg.Registry
	if selectedMode.runsTurnsLocally(*aetherAddr) {
		modelRegistry, err = loadAppModelRegistry(*workspace)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error: load model registry:", err)
			os.Exit(2)
		}
	}
	if err := setupAppLogging(*workspace, selectedMode == appModeTUI); err != nil {
		fmt.Fprintln(os.Stderr, "error: setup logging:", err)
		os.Exit(1)
	}

	// Install the OTel tracer/exporter (no-op unless SAHARA_TRACING_ENABLED + an
	// OTLP endpoint are set). A tracing init failure must not stop the agent.
	shutdownTracing, terr := otlpexport.Init(context.Background())
	if terr != nil {
		fmt.Fprintln(os.Stderr, "warning: tracing init:", terr)
	}

	cfg := appConfig{
		workspaceRoot:     *workspace,
		workspaceID:       workspaceResolution.WorkspaceID,
		workspaceIndexDir: *workspaceIndexDir,
		visibleWorkspaces: parseVisibleWorkspaces(*visibleWorkspaces),
		stateDir:          stateDir,
		thread:            *thread,
		baseURL:           *baseURL,
		model:             resolveAppModel(*model, modelRegistry),
		modelRegistry:     modelRegistry,
		llmFormat:         *llmFormat,
		seed:              *seed,
		record:            *record,

		aetherAddr:                  *aetherAddr,
		aetherWorkspace:             effectiveWorkspace(*aetherWorkspace, workspaceResolution.WorkspaceID),
		aetherSpecifier:             *aetherSpecifier,
		aetherTLS:                   *aetherTLS,
		aetherTLSInsecure:           *aetherTLSInsecure,
		aetherTaskMessageLanes:      *aetherTaskMessageLanes,
		subagentTarget:              strings.TrimSpace(*subagentTarget),
		subagentExecutor:            *subagentExecutor,
		subagentExecutorConcurrency: *subagentExecutorConcurrency,
		aetherUser:                  *aetherUser,
		aetherWindow:                resolveWindowID(*aetherWindow),

		memorylayerMode:              normalizedMemoryLayerMode,
		memorylayerURL:               strings.TrimSpace(*memorylayerURL),
		memorylayerTarget:            strings.TrimSpace(*memorylayerTarget),
		memorylayerRequired:          requiresMemoryLayer,
		memorylayerKey:               os.Getenv("MEMORYLAYER_API_KEY"),
		memorylayerWorkspace:         effectiveWorkspace(*memorylayerWorkspace, workspaceResolution.WorkspaceID),
		promptNotesAuthority:         normalizedPromptNotesAuthority,
		agentSpecificationsAuthority: normalizedAgentSpecificationsAuthority,
		refinementAuthority:          normalizedRefinementAuthority,
		refinementSessionAutoApply:   *refinementSessionAutoApply,
		memoryRecall:                 *memoryRecall,
		memoryRecallLimit:            *memoryRecallLimit,
		goalMaxContinuations:         uint32(*goalMaxContinuations),
	}
	if selectedMode == appModeServe || selectedMode == appModeStandalone {
		cfg.streamFlush = aetherStreamFlush
	}

	switch selectedMode {
	case appModeCLI:
		err = runCLI(cfg)
	case appModeTUI:
		if cfg.aetherAddr != "" {
			err = runTUIClient(cfg)
		} else {
			err = runTUI(cfg)
		}
	case appModeACP:
		err = runACP(cfg)
	case appModeWeb:
		err = runWeb(cfg, *addr, !*noBrowser)
	case appModeServe:
		err = runServe(cfg)
	case appModeStandalone:
		err = runAetherStandalone(cfg)
	}
	// Flush + stop the trace exporter before exit (os.Exit skips defers, so do it
	// explicitly). Bounded so a stuck collector can't hang shutdown.
	if shutdownTracing != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracing(sctx)
		cancel()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type appMode string

const (
	appModeTUI        appMode = "tui"
	appModeCLI        appMode = "cli"
	appModeACP        appMode = "acp"
	appModeWeb        appMode = "web"
	appModeServe      appMode = "serve"
	appModeStandalone appMode = "aether-standalone"
)

// needsAether reports whether the mode is meaningless without a gateway.
func (m appMode) needsAether() bool {
	return m == appModeServe || m == appModeStandalone
}

// runsTurnsLocally reports whether this process executes turns itself (and so
// needs a provider endpoint). Only a UI attached to a remote agent does not.
func (m appMode) runsTurnsLocally(aetherAddr string) bool {
	if m == appModeTUI && aetherAddr != "" {
		return false
	}
	return true
}

// usesAether reports whether this mode actually opens an Aether connection.
// Merely passing --aether to an in-process CLI/web/ACP mode has never changed
// its turn transport, so it must not accidentally activate service discovery.
func (m appMode) usesAether(aetherAddr string) bool {
	return m == appModeServe || m == appModeStandalone || (m == appModeTUI && aetherAddr != "")
}

func selectAppMode(cli, tui, acp, web, serve, standalone bool) (appMode, error) {
	if nTrue(cli, tui, acp, web, serve, standalone) > 1 {
		return "", fmt.Errorf("choose only one of --cli, --tui, --acp, --web, --serve, or --aether-standalone")
	}
	switch {
	case cli:
		return appModeCLI, nil
	case acp:
		return appModeACP, nil
	case web:
		return appModeWeb, nil
	case serve:
		return appModeServe, nil
	case standalone:
		return appModeStandalone, nil
	default:
		return appModeTUI, nil
	}
}

// nTrue counts the set flags among the given booleans (mode mutual-exclusion).
func nTrue(flags ...bool) int {
	n := 0
	for _, f := range flags {
		if f {
			n++
		}
	}
	return n
}
