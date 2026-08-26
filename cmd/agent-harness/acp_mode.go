package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channels/acp"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// runACP serves the harness as an Agent Client Protocol (ACP) agent over stdio:
// JSON-RPC 2.0 rides stdin/stdout, so all human-facing status stays on stderr.
// An editor/host (e.g. Zed) speaks initialize → session/new → session/prompt and
// receives streamed session/update notifications mapped from the turn's events.
func runACP(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ac := acp.NewChannel(os.Stdin, os.Stdout)
	// The broker bridges tool-approval prompts to the client: the runner blocks on
	// Await while the channel drives a session/request_permission round-trip and
	// calls Resolve with the outcome.
	broker := approval.New()
	// ac.TurnContext routes the harness file/shell tools through the ACP client's
	// fs/terminal capabilities when the client advertised them.
	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	runner, _, err := buildRunner(cfg, st, ac, broker, nil, nil, nil, nil, ac.TurnContext, nil)
	if err != nil {
		return err
	}
	ac.SetApprovalResolver(broker)
	canceller := turncancel.New()
	// Bridge ACP session/cancel to the runtime canceller: it keys in-flight turns
	// by task id, so an out-of-band cancel aborts the running turn's context.
	ac.SetCanceller(func(id string) { canceller.Cancel(id) })
	rt, err := runtime.NewRunner(ac, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)
	rt.SetSteering(runner.SteeringInbox())

	// The JSON-RPC read loop feeds inbound prompts to the runtime's FetchTask; run
	// the turn loop concurrently so streaming session/update notifications flow while
	// stdin stays free to receive session/cancel.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 4}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "run loop: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s - ACP agent on stdio\n", cfg.model, cfg.baseURL)
	serveErr := ac.Serve(ctx)
	stop()
	<-done
	if serveErr != nil && ctx.Err() == nil {
		return serveErr
	}
	return nil
}
