package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const defaultHookTimeout = 60 * time.Second

type Executor struct {
	cfg Config
}

func NewExecutor(cfg Config) *Executor {
	return &Executor{cfg: cfg}
}

func (e *Executor) Dispatch(ctx context.Context, inv Invocation, hooks []CommandHook) ([]Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	matching := matchingHooks(inv.Event, hooks)
	if recursionGuarded(ctx) {
		return skippedResults(inv.Event, matching), nil
	}
	ctx = WithRecursionGuard(ctx)
	results := make([]Result, 0, len(matching))
	for _, hook := range matching {
		result, err := e.runCommandHook(ctx, inv, hook)
		if err != nil {
			return results, err
		}
		results = append(results, result)
		if result.Blocked {
			break
		}
	}
	return results, nil
}

func (e *Executor) runCommandHook(ctx context.Context, inv Invocation, hook CommandHook) (Result, error) {
	if len(hook.Command) == 0 || hook.Command[0] == "" {
		return Result{}, ErrInvalidHookCommand
	}
	hookCtx, cancel := context.WithTimeout(ctx, hookTimeout(e.cfg.DefaultTimeout, hook.Timeout))
	defer cancel()
	input, err := hookInput(inv)
	if err != nil {
		return Result{}, err
	}
	cmd := exec.CommandContext(hookCtx, hook.Command[0], hook.Command[1:]...)
	cmd.Stdin = bytes.NewReader(input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = e.hookEnv(hook)
	runErr := cmd.Run()
	if hookCtx.Err() != nil {
		if errors.Is(hookCtx.Err(), context.DeadlineExceeded) {
			return Result{}, ErrHookTimeout
		}
		return Result{}, hookCtx.Err()
	}
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return Result{}, fmt.Errorf("run hook %s: %w", hook.Name, runErr)
		}
	}
	result, err := commandResult(inv.Event, hook, exitCode, stdout.String(), stderr.String())
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func (e *Executor) hookEnv(hook CommandHook) []string {
	env := make([]string, 0, len(e.cfg.EnvAllowlist)+len(hook.Env))
	for _, key := range e.cfg.EnvAllowlist {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	for key, value := range hook.Env {
		env = append(env, key+"="+value)
	}
	return env
}

func commandResult(event EventName, hook CommandHook, exitCode int, stdout string, stderr string) (Result, error) {
	output, err := parseHookOutput(stdout)
	if err != nil {
		return Result{}, err
	}
	result := Result{
		HookName: hook.Name,
		Event:    event,
		ExitCode: exitCode,
		Stdout:   stdout,
		Stderr:   stderr,
		Output:   output,
	}
	result.Blocked = exitCode == 2 || output.Decision == DecisionDeny
	if result.Blocked {
		result.Reason = blockReason(output, stderr, stdout)
	}
	return result, nil
}

func parseHookOutput(stdout string) (HookOutput, error) {
	raw := strings.TrimSpace(stdout)
	if raw == "" {
		return HookOutput{}, nil
	}
	var output HookOutput
	if err := json.Unmarshal([]byte(raw), &output); err != nil {
		return HookOutput{}, fmt.Errorf("%w: %v", ErrMalformedHookOutput, err)
	}
	return output, nil
}

func hookInput(inv Invocation) ([]byte, error) {
	if len(inv.Input) > 0 {
		return json.Marshal(inv)
	}
	return json.Marshal(Invocation{Event: inv.Event, Tool: inv.Tool})
}

func matchingHooks(event EventName, hooks []CommandHook) []CommandHook {
	matching := make([]CommandHook, 0, len(hooks))
	for _, hook := range hooks {
		if hook.Event == event {
			matching = append(matching, hook)
		}
	}
	return matching
}

func skippedResults(event EventName, hooks []CommandHook) []Result {
	results := make([]Result, 0, len(hooks))
	for _, hook := range hooks {
		results = append(results, Result{HookName: hook.Name, Event: event, Skipped: true})
	}
	return results
}

func hookTimeout(defaultTimeout time.Duration, hookTimeout time.Duration) time.Duration {
	if hookTimeout > 0 {
		return hookTimeout
	}
	if defaultTimeout > 0 {
		return defaultTimeout
	}
	return defaultHookTimeout
}

func blockReason(output HookOutput, stderr string, stdout string) string {
	switch {
	case output.Reason != "":
		return output.Reason
	case strings.TrimSpace(stderr) != "":
		return strings.TrimSpace(stderr)
	case strings.TrimSpace(stdout) != "":
		return strings.TrimSpace(stdout)
	default:
		return "blocked by hook"
	}
}
