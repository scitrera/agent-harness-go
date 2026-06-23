// Command agent-harness is the reference CLI for the OSS agent-harness-go core.
// It runs an interactive chat against an OpenAI-compatible endpoint using only
// core packages + reference impls (file store, filesystem skills/commands, CLI
// publisher) — no Scitrera transport or memory backend. It demonstrates how to
// wire the seams; the Scitrera distribution swaps in Aether + MemoryLayer.
//
// Configuration is via env (SAHARA_* prefix — the project codename) with flag
// overrides:
//
//	SAHARA_WORKSPACE_ROOT   workspace dir (default ./workspace)
//	SAHARA_STATE_DIR        history/state dir (default <workspace>/.agent-harness)
//	SAHARA_LLM_BASE_URL     OpenAI-compatible base URL (required)
//	SAHARA_LLM_API_KEY      bearer key
//	SAHARA_LLM_MODEL        model id (default gpt-4o-mini)
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/channels/cli"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func main() {
	workspace := flag.String("workspace", env("SAHARA_WORKSPACE_ROOT", "./workspace"), "workspace root")
	thread := flag.String("thread", "cli", "chat thread id")
	baseURL := flag.String("base-url", os.Getenv("SAHARA_LLM_BASE_URL"), "OpenAI-compatible base URL")
	model := flag.String("model", env("SAHARA_LLM_MODEL", "gpt-4o-mini"), "model id")
	seed := flag.Bool("seed", true, "seed default workspace files when missing")
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
	if err := run(*workspace, *thread, *baseURL, *model, *seed); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(workspaceRoot, thread, baseURL, model string, seed bool) error {
	ctx := context.Background()
	stateDir := env("SAHARA_STATE_DIR", filepath.Join(workspaceRoot, ".agent-harness"))
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		return err
	}
	if seed {
		seedDefaults(workspaceRoot)
	}

	ws, err := localtools.NewWorkspace(workspaceRoot)
	if err != nil {
		return fmt.Errorf("workspace: %w", err)
	}
	exa, err := localtools.NewExaClient("https://api.exa.ai/search", os.Getenv("EXA_API_KEY"), true, nil)
	if err != nil {
		return fmt.Errorf("exa: %w", err)
	}
	reg := tools.NewRegistry() // permissive (no policy) — the OSS default
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws, Python: "python3", Exa: exa, Timeout: 30 * time.Second, MaxOutput: 1 << 20}); err != nil {
		return fmt.Errorf("register tools: %w", err)
	}

	auth := ""
	if k := os.Getenv("SAHARA_LLM_API_KEY"); k != "" {
		auth = "Bearer " + k
	}
	prov, err := provider.NewSidecarClient(provider.SidecarConfig{BaseURL: baseURL, AuthHeader: auth, Format: provider.FormatOpenAI})
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}

	fsStore := store.NewFileStore(workspaceRoot, stateDir)
	skillSpecs, _ := skills.Discover(workspaceRoot, []string{"skills", ".agent-harness-skills"})
	cmdSpecs, _ := commands.Discover(workspaceRoot, []string{"commands", ".agent-harness-commands"})

	runner, err := turn.NewRunner(turn.Config{
		Store:     fsStore,
		Loader:    fsStore,
		Registry:  reg,
		Provider:  prov,
		Publisher: cli.NewPublisher(os.Stdout),
		Assembler: contextpack.NewAssembler(contextpack.Config{
			MaxHistoryMessages: 24,
			MaxTextPartBytes:   64 << 10,
			MaxFileBytes:       16 << 10,
			Skills:             skillSummaries(skillSpecs),
			WorkspaceDir:       workspaceRoot,
			Model:              model,
			Now:                time.Now,
		}),
		Model:     model,
		Streaming: true,
		Commands:  commands.New(cmdSpecs),
		Now:       time.Now,
	})
	if err != nil {
		return fmt.Errorf("runner: %w", err)
	}

	addr := protocol.MessageAddress{ThreadID: thread}
	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s thread=%s — type a message, Ctrl-D to exit\n", model, baseURL, thread)
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
