package tools

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

func shell(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Command   string   `json:"command"`
		Args      []string `json:"args"`
		CWD       string   `json:"cwd"`
		Env       []string `json:"env"`
		TimeoutMS int64    `json:"timeout_ms"`
		MaxOutput int      `json:"max_output"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.CWD = ResolveWorkingPath(ctx, args.CWD)
	command, commandArgs := shellCommand(args.Command, args.Args)
	spec := localtools.CommandSpec{
		Name:         command,
		Args:         commandArgs,
		CWD:          args.CWD,
		Env:          args.Env,
		Timeout:      durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput:    outputLimit(args.MaxOutput, cfg.MaxOutput),
		ArchiveLimit: archiveLimit(cfg),
	}
	if err := enforceCommandPolicy(cfg, req, spec); err != nil {
		return Result{}, err
	}
	result, err := runCommand(ctx, cfg, spec)
	if err != nil && result.PID == 0 {
		return Result{}, err
	}
	out, resultErr := commandResult(req, result, archiveOutput(ctx, cfg, req, result))
	if resultErr != nil {
		return Result{}, resultErr
	}
	if err != nil {
		return out, err
	}
	return out, nil
}

func shellCommand(command string, args []string) (string, []string) {
	if len(args) == 0 && commandNeedsInterpreter(command) {
		return "/bin/sh", []string{"-lc", command}
	}
	return command, args
}

func commandNeedsInterpreter(command string) bool {
	command = strings.TrimSpace(command)
	return strings.IndexFunc(command, func(value rune) bool {
		return value == ' ' || value == '\t' || value == '\n' || strings.ContainsRune("|&;<>()$`\\*?[]{}~'\"=#", value)
	}) >= 0
}

func python(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Code      string `json:"code"`
		CWD       string `json:"cwd"`
		TimeoutMS int64  `json:"timeout_ms"`
		MaxOutput int    `json:"max_output"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.CWD = ResolveWorkingPath(ctx, args.CWD)
	pythonPath := cfg.Python
	if pythonPath == "" {
		pythonPath = "python3"
	}
	spec := localtools.CommandSpec{
		Name:         pythonPath,
		Args:         []string{"-c", args.Code},
		CWD:          args.CWD,
		Timeout:      durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput:    outputLimit(args.MaxOutput, cfg.MaxOutput),
		ArchiveLimit: archiveLimit(cfg),
	}
	if err := enforceCommandPolicy(cfg, req, spec); err != nil {
		return Result{}, err
	}
	result, err := runCommand(ctx, cfg, spec)
	if err != nil && result.PID == 0 {
		return Result{}, err
	}
	out, resultErr := commandResult(req, result, archiveOutput(ctx, cfg, req, result))
	if resultErr != nil {
		return Result{}, resultErr
	}
	if err != nil {
		return out, err
	}
	return out, nil
}

// runCommand executes spec via a ctx-carried CommandDelegate (ACP terminal/*)
// when present, else the local workspace. The command policy is already enforced
// by the caller — the delegate never sees an un-gated command.
func runCommand(ctx context.Context, cfg LocalConfig, spec localtools.CommandSpec) (localtools.CommandResult, error) {
	if d := CommandDelegateFrom(ctx); d != nil {
		return d.RunCommand(ctx, spec)
	}
	return cfg.Workspace.RunCommand(ctx, spec)
}

func webSearch(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Query string `json:"query"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	result, err := cfg.Exa.Search(ctx, args.Query)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, struct {
		Body string `json:"body"`
	}{Body: result.Body})
}

func enforceCommandPolicy(cfg LocalConfig, req Request, spec localtools.CommandSpec) error {
	if cfg.CommandPolicy == nil {
		return nil
	}
	argv := append([]string{spec.Name}, spec.Args...)
	decision := cfg.CommandPolicy.DecideCommand(CommandRequest{
		Argv:          argv,
		WorkspaceRoot: cfg.Workspace.Root(),
		CWD:           spec.CWD,
		Env:           spec.Env,
	})
	if !decision.Allowed() {
		return policyError(decision.Decision, req.Name)
	}
	return nil
}

func durationFromMillis(ms int64, fallback time.Duration) time.Duration {
	if ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return fallback
}

func outputLimit(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// archiveLimit resolves the per-call recovery-archive budget: 0 means "use the
// default", negative means the operator turned archiving off.
func archiveLimit(cfg LocalConfig) int {
	if cfg.OutputArchiveLimit < 0 {
		return 0
	}
	if cfg.OutputArchiveLimit == 0 {
		return DefaultOutputArchiveLimit
	}
	return cfg.OutputArchiveLimit
}

// archiveOutput writes a truncated command's retained output to the workspace and
// returns the ref the model reads back with read_file. It returns "" whenever
// there is nothing to archive or the write fails — the truncation marker is
// emitted either way, so a failed archive degrades to "we told you it was cut"
// rather than to silence. The error is logged, never surfaced as a tool failure:
// the command itself succeeded or failed on its own terms and that verdict must
// not be rewritten by a bookkeeping problem.
func archiveOutput(ctx context.Context, cfg LocalConfig, req Request, result localtools.CommandResult) string {
	if result.Archive == "" || cfg.Workspace == nil {
		return ""
	}
	key := req.CallID
	if key == "" {
		key = req.Name
	}
	ref, err := localtools.NewEvictionSink(cfg.Workspace, archiveDir(cfg)).Put(ctx, key, []byte(result.Archive))
	if err != nil {
		slog.WarnContext(ctx, "tool output archive write failed",
			slog.String("tool", req.Name), slog.Any("err", err))
		return ""
	}
	return ref
}

func archiveDir(cfg LocalConfig) string {
	if strings.TrimSpace(cfg.OutputArchiveDir) == "" {
		return DefaultOutputArchiveDir
	}
	return cfg.OutputArchiveDir
}
