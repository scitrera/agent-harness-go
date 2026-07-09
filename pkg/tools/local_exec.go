package tools

import (
	"context"
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
	spec := localtools.CommandSpec{
		Name:      args.Command,
		Args:      args.Args,
		CWD:       args.CWD,
		Env:       args.Env,
		Timeout:   durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput: outputLimit(args.MaxOutput, cfg.MaxOutput),
	}
	if err := enforceCommandPolicy(cfg, req, spec); err != nil {
		return Result{}, err
	}
	result, err := runCommand(ctx, cfg, spec)
	if err != nil && result.PID == 0 {
		return Result{}, err
	}
	out, resultErr := commandResult(req, result)
	if resultErr != nil {
		return Result{}, resultErr
	}
	if err != nil {
		return out, err
	}
	return out, nil
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
	pythonPath := cfg.Python
	if pythonPath == "" {
		pythonPath = "python3"
	}
	spec := localtools.CommandSpec{
		Name:      pythonPath,
		Args:      []string{"-c", args.Code},
		CWD:       args.CWD,
		Timeout:   durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput: outputLimit(args.MaxOutput, cfg.MaxOutput),
	}
	if err := enforceCommandPolicy(cfg, req, spec); err != nil {
		return Result{}, err
	}
	result, err := runCommand(ctx, cfg, spec)
	if err != nil && result.PID == 0 {
		return Result{}, err
	}
	out, resultErr := commandResult(req, result)
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
