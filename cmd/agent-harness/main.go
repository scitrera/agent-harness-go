// Command agent-harness is the reference CLI for the OSS agent-harness-go core.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
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

	cfg := appConfig{
		workspaceRoot: *workspace,
		stateDir:      env("SAHARA_STATE_DIR", filepath.Join(*workspace, ".agent-harness")),
		thread:        *thread,
		baseURL:       *baseURL,
		model:         *model,
		seed:          *seed,
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
