package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// runAetherStandalone runs both halves in one process: the agent worker in the
// background and the terminal UI in the foreground, talking to each other
// through the gateway rather than through a Go channel.
//
// It is the same split deployment as `--serve` plus `--tui --aether`, minus the
// second terminal — useful to see the real transport in the loop without
// standing up two processes. The two halves still exchange every turn over
// Aether, so nothing about the path is special-cased.
func runAetherStandalone(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A per-process specifier keeps two standalone runs on one gateway from
	// answering each other's turns.
	if cfg.aetherSpecifier == "" {
		specifier, err := ids.New("standalone-")
		if err != nil {
			return fmt.Errorf("specifier: %w", err)
		}
		cfg.aetherSpecifier = specifier
	}

	broker := approval.New()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr:            cfg.aetherAddr,
		Workspace:             cfg.aetherWorkspace,
		Specifier:             cfg.aetherSpecifier,
		SourceAgent:           cfg.sourceAgent(),
		APIKey:                os.Getenv("AETHER_API_KEY"),
		Token:                 os.Getenv("AETHER_TOKEN"),
		Tenant:                os.Getenv("AETHER_TENANT"),
		TLSEnabled:            cfg.aetherTLS,
		TLSInsecureSkipVerify: cfg.aetherTLSInsecure,
	})
	if err != nil {
		return err
	}
	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	runner, _, err := buildRunner(cfg, st, worker, broker, nil)
	if err != nil {
		return err
	}
	canceller := turncancel.New()
	worker.SetCanceller(canceller)
	worker.SetApprovalBroker(broker)
	worker.SetThreadClearer(func(addr protocol.MessageAddress) error {
		return deleteAddressHistory(context.Background(), st.history, addr)
	})
	rt, err := runtime.NewRunner(worker, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)
	if err := worker.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = worker.Close() }()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "run loop: %v\n", err)
		}
	}()

	uiErr := runTUIClient(cfg)

	stop()
	select {
	case <-workerDone:
	case <-time.After(shutdownTimeout):
		fmt.Fprintln(os.Stderr, "shutdown: worker did not drain in-flight turn within timeout")
	}
	return uiErr
}
