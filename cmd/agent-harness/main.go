// Command agent-harness is the reference CLI for the OSS agent-harness-go core.
// It runs an interactive chat against an OpenAI-compatible endpoint using only
// core packages + reference impls (file store, filesystem skills/commands,
// web/CLI publishers) — no Scitrera transport or memory backend. It demonstrates
// how to wire the seams; the Scitrera distribution swaps in Aether + MemoryLayer.
//
// By default it serves a browser chat UI (the web channel). Pass --cli to run
// the original stdin REPL instead.
//
// Configuration is via env (SAHARA_* prefix — the project codename) with flag
// overrides:
//
//	SAHARA_WORKSPACE_ROOT   workspace dir (default ./workspace)
//	SAHARA_STATE_DIR        history/state dir (default <workspace>/.agent-harness)
//	SAHARA_LLM_BASE_URL     OpenAI-compatible base URL (required)
//	SAHARA_LLM_API_KEY      bearer key
//	SAHARA_LLM_MODEL        model id (default gpt-4o-mini)
//	SAHARA_WEB_ADDR         web UI listen address (default 127.0.0.1:8787)
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/channels/cli"
	"github.com/scitrera/agent-harness-go/pkg/channels/web"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// appConfig holds the resolved shared configuration for both modes.
type appConfig struct {
	workspaceRoot string
	stateDir      string
	thread        string
	baseURL       string
	model         string
	seed          bool
}

func main() {
	workspace := flag.String("workspace", env("SAHARA_WORKSPACE_ROOT", "./workspace"), "workspace root")
	thread := flag.String("thread", "cli", "chat thread id (CLI mode)")
	baseURL := flag.String("base-url", os.Getenv("SAHARA_LLM_BASE_URL"), "OpenAI-compatible base URL")
	model := flag.String("model", env("SAHARA_LLM_MODEL", "gpt-4o-mini"), "model id")
	seed := flag.Bool("seed", true, "seed default workspace files when missing")
	cliMode := flag.Bool("cli", false, "run the stdin REPL instead of the web UI")
	addr := flag.String("addr", env("SAHARA_WEB_ADDR", "127.0.0.1:8787"), "web UI listen address")
	noBrowser := flag.Bool("no-browser", false, "do not open a browser (web mode)")
	// Present flags GNU-style (--flag). Go's flag package already accepts both
	// -flag and --flag; this only changes the help display.
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

	cfg := appConfig{
		workspaceRoot: *workspace,
		stateDir:      env("SAHARA_STATE_DIR", filepath.Join(*workspace, ".agent-harness")),
		thread:        *thread,
		baseURL:       *baseURL,
		model:         *model,
		seed:          *seed,
	}

	var err error
	if *cliMode {
		err = runCLI(cfg)
	} else {
		err = runWeb(cfg, *addr, !*noBrowser)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// buildRunner constructs the turn runner + file store shared by both modes. The
// only per-mode difference is the egress publisher (CLI stdout vs web SSE).
func buildRunner(cfg appConfig, pub channel.Publisher) (*turn.Runner, *store.FileStore, error) {
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
	reg := tools.NewRegistry() // permissive (no policy) — the OSS default
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws, Python: "python3", Exa: exa, Timeout: 30 * time.Second, MaxOutput: 1 << 20}); err != nil {
		return nil, nil, fmt.Errorf("register tools: %w", err)
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
	skillSpecs, _ := skills.Discover(cfg.workspaceRoot, []string{"skills", ".agent-harness-skills"})
	cmdSpecs, _ := commands.Discover(cfg.workspaceRoot, []string{"commands", ".agent-harness-commands"})

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
			Skills:             skillSummaries(skillSpecs),
			WorkspaceDir:       cfg.workspaceRoot,
			Model:              cfg.model,
			Now:                time.Now,
		}),
		Model:     cfg.model,
		Streaming: true,
		Commands:  commands.New(cmdSpecs),
		Now:       time.Now,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("runner: %w", err)
	}
	return runner, fsStore, nil
}

// runWeb serves the browser chat UI. Posted messages enqueue onto the web
// channel; runtime.RunLoop drives turns (per-thread in order, distinct threads
// concurrently) and the turn runner streams events back over SSE.
func runWeb(cfg appConfig, addr string, openBrowser bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wc := web.NewChannel()
	runner, fsStore, err := buildRunner(cfg, wc)
	if err != nil {
		return err
	}
	sessions, err := web.NewIndex(cfg.stateDir, time.Now)
	if err != nil {
		return fmt.Errorf("sessions: %w", err)
	}
	canceller := turncancel.New()
	rt, err := runtime.NewRunner(wc, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	srv := web.New(wc, fsStore, sessions, canceller)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	go func() {
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "run loop: %v\n", err)
		}
	}()

	url := "http://" + addr
	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s — web UI at %s\n", cfg.model, cfg.baseURL, url)
	if openBrowser {
		_ = web.OpenBrowser(url)
	}

	errc := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errc <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutCtx)
}

// runCLI runs the original stdin REPL, calling the turn runner directly.
func runCLI(cfg appConfig) error {
	ctx := context.Background()
	runner, _, err := buildRunner(cfg, cli.NewPublisher(os.Stdout))
	if err != nil {
		return err
	}

	addr := protocol.MessageAddress{ThreadID: cfg.thread}
	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s thread=%s — type a message, Ctrl-D to exit\n", cfg.model, cfg.baseURL, cfg.thread)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	turnNo := 0
	for {
		fmt.Fprint(os.Stderr, "\nyou> ")
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		turnNo++
		part, err := protocol.NewTextPart(line)
		if err != nil {
			return err
		}
		user := protocol.ChatMessage{ID: fmt.Sprintf("user-%d", turnNo), Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{part}}
		fmt.Fprint(os.Stdout, "\nassistant> ")
		if _, err := runner.Run(ctx, addr, user); err != nil {
			fmt.Fprintf(os.Stderr, "\nturn error: %v\n", err)
		}
	}
	return sc.Err()
}

func seedDefaults(root string) {
	for _, f := range sysprompt.DefaultWorkspaceFiles() {
		path := filepath.Join(root, f.Name)
		if _, err := os.Stat(path); err == nil {
			continue // no-clobber
		}
		_ = os.WriteFile(path, []byte(f.Content), 0o644)
	}
}

func skillSummaries(specs []catalog.SkillSpec) []sysprompt.SkillSummary {
	out := make([]sysprompt.SkillSummary, 0, len(specs))
	for _, s := range specs {
		if !s.Enabled {
			continue
		}
		out = append(out, sysprompt.SkillSummary{Name: s.Name, Description: s.Description, Path: s.Path})
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
