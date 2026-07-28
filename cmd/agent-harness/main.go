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
)

func main() {
	workspace := flag.String("workspace", env("SAHARA_WORKING_DIRECTORY", "./workspace"), "workspace root")
	thread := flag.String("thread", "cli", "chat thread id (CLI/TUI mode)")
	baseURL := flag.String("base-url", os.Getenv("SAHARA_LLM_BASE_URL"), "OpenAI-compatible base URL")
	model := flag.String("model", env("SAHARA_LLM_MODEL", "gpt-4o-mini"), "model id")
	seed := flag.Bool("seed", true, "seed default workspace files when missing")
	cliMode := flag.Bool("cli", false, "run the stdin REPL instead of the web UI")
	tuiMode := flag.Bool("tui", false, "run the terminal UI instead of the web UI")
	acpMode := flag.Bool("acp", false, "run as an Agent Client Protocol (ACP) agent over stdio")
	addr := flag.String("addr", env("SAHARA_WEB_ADDR", "127.0.0.1:8787"), "web UI listen address (NO auth - localhost only)")
	noBrowser := flag.Bool("no-browser", false, "do not open a browser (web mode)")
	exportThread := flag.String("export", "", "export thread history as JSONL to stdout and exit (a thread id, or 'all')")
	exportFormat := flag.String("export-format", "openai", "export schema: openai (chat SFT) | trace (lossless spec messages)")
	record := flag.String("record", "", "record each LLM call (assembled prompt + response + usage) as JSONL to this path; off by default")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: agent-harness [flags]\n\nFlags:\n")
		flag.VisitAll(func(f *flag.Flag) {
			def := ""
			if f.DefValue != "" {
				def = fmt.Sprintf(" (default %q)", f.DefValue)
			}
			fmt.Fprintf(flag.CommandLine.Output(), "  --%-12s %s%s\n", f.Name, f.Usage, def)
		})
	}
	flag.Parse()

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

	if *baseURL == "" {
		fmt.Fprintln(os.Stderr, "error: set --base-url or SAHARA_LLM_BASE_URL (an OpenAI-compatible endpoint)")
		os.Exit(2)
	}
	if nTrue(*cliMode, *tuiMode, *acpMode) > 1 {
		fmt.Fprintln(os.Stderr, "error: choose only one of --cli, --tui, or --acp")
		os.Exit(2)
	}
	if err := setupAppLogging(*workspace, *tuiMode); err != nil {
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
	}

	var err error
	switch {
	case *cliMode:
		err = runCLI(cfg)
	case *tuiMode:
		err = runTUI(cfg)
	case *acpMode:
		err = runACP(cfg)
	default:
		err = runWeb(cfg, *addr, !*noBrowser)
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
