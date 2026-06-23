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
	result, err := cfg.Workspace.RunCommand(ctx, localtools.CommandSpec{
		Name:      args.Command,
		Args:      args.Args,
		CWD:       args.CWD,
		Env:       args.Env,
		Timeout:   durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput: outputLimit(args.MaxOutput, cfg.MaxOutput),
	})
	if err != nil {
		return Result{}, err
	}
	return commandResult(req, result)
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
	result, err := cfg.Workspace.RunPython(ctx, cfg.Python, args.Code, localtools.CommandSpec{
		CWD:       args.CWD,
		Timeout:   durationFromMillis(args.TimeoutMS, cfg.Timeout),
		MaxOutput: outputLimit(args.MaxOutput, cfg.MaxOutput),
	})
	if err != nil {
		return Result{}, err
	}
	return commandResult(req, result)
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
