// Command agent-harness is the reference CLI for the OSS agent-harness-go core.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/telemetry/otlpexport"
	"github.com/scitrera/agent-harness-go/pkg/version"
)

func main() {
	workspace := flag.String("workspace", env("SAHARA_WORKING_DIRECTORY", "./workspace"), "workspace root")
	thread := flag.String("thread", "cli", "chat thread id (CLI/TUI mode)")
	baseURL := flag.String("base-url", os.Getenv("SAHARA_LLM_BASE_URL"), "OpenAI-compatible base URL")
	model := flag.String("model", env("SAHARA_LLM_MODEL", "gpt-4o-mini"), "model id")
	seed := flag.Bool("seed", true, "seed default workspace files when missing")
	cliMode := flag.Bool("cli", false, "run the stdin REPL")
	tuiMode := flag.Bool("tui", false, "run the terminal UI (default)")
	acpMode := flag.Bool("acp", false, "run as an Agent Client Protocol (ACP) agent over stdio")
	webMode := flag.Bool("web", false, "run the localhost web UI")
	serveMode := flag.Bool("serve", false, "run as a headless agent worker reachable over Aether (requires --aether)")
	standalone := flag.Bool("aether-standalone", false, "run the agent worker and the terminal UI in one process, both over Aether (requires --aether)")
	aetherAddr := flag.String("aether", os.Getenv("AETHER_ADDR"), "Aether gateway address, e.g. 127.0.0.1:50051")
	aetherWorkspace := flag.String("aether-workspace", env("AETHER_WORKSPACE", "default"), "Aether workspace to join")
	aetherSpecifier := flag.String("aether-specifier", os.Getenv("AETHER_SPECIFIER"), "Aether agent-topic specifier (distinguishes instances)")
	aetherUser := flag.String("aether-user", env("AETHER_USER", defaultAetherUser()), "client user id (Aether client modes)")
	aetherWindow := flag.String("aether-window", os.Getenv("AETHER_WINDOW"), "client window id; defaults to a fresh id per process")
	aetherTLS := flag.Bool("aether-tls", false, "use TLS for the Aether connection")
	aetherTLSInsecure := flag.Bool("aether-tls-insecure", false, "skip Aether TLS certificate verification (testing only)")
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

	// Export mode reads local state only (no provider needed): dump thread history
	// as JSONL and exit, before the base-url requirement below.
	if *exportThread != "" {
		stateDir := env("SAHARA_STATE_DIR", filepath.Join(*workspace, ".agent-harness"))
		if err := runExport(stateDir, *exportThread, *exportFormat, os.Stdout); err != nil {
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
	// A pure client drives someone else's agent, so it needs no provider of its
	// own; every other mode runs turns locally and does.
	if *baseURL == "" && selectedMode.runsTurnsLocally(*aetherAddr) {
		fmt.Fprintln(os.Stderr, "error: set --base-url or SAHARA_LLM_BASE_URL (an OpenAI-compatible endpoint)")
		os.Exit(2)
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
		workspaceRoot: *workspace,
		stateDir:      env("SAHARA_STATE_DIR", filepath.Join(*workspace, ".agent-harness")),
		thread:        *thread,
		baseURL:       *baseURL,
		model:         *model,
		seed:          *seed,
		record:        *record,

		aetherAddr:        *aetherAddr,
		aetherWorkspace:   *aetherWorkspace,
		aetherSpecifier:   *aetherSpecifier,
		aetherTLS:         *aetherTLS,
		aetherTLSInsecure: *aetherTLSInsecure,
		aetherUser:        *aetherUser,
		aetherWindow:      resolveWindowID(*aetherWindow),
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
