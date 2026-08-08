package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/channels/web"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

func runWeb(cfg appConfig, addr string, openBrowser bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wc := web.NewChannel()
	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	wireWorkspaceID := effectiveWorkspace("", cfg.workspaceID)
	workspaceResolver, err := newSessionWorkspaceResolver(wireWorkspaceID, cfg.visibleWorkspaces)
	if err != nil {
		return err
	}
	sessionTransport, err := newSessionTransport(
		cfg.stateDir, st, nil, workspaceResolver, wireWorkspaceID,
		hasAdditionalVisibleWorkspace(wireWorkspaceID, cfg.visibleWorkspaces), wc, wc,
	)
	if err != nil {
		return err
	}
	// Preserve the web channel's inbound Enqueue capability while wrapping only
	// its publisher side with cursor-bearing recording.
	runnerChannel := publisherEnqueuer{Publisher: sessionTransport.Publisher, Enqueuer: wc}
	runner, _, err := buildRunner(cfg, st, runnerChannel, nil, nil)
	if err != nil {
		return err
	}
	// The REST history surface uses the same bound store as the runner so project
	// mode cannot display or delete another workspace's transcript.
	sessions, err := threadindex.NewIndex(workspaceStateDir(cfg), time.Now)
	if err != nil {
		return fmt.Errorf("sessions: %w", err)
	}
	canceller := turncancel.New()
	rt, err := runtime.NewRunner(wc, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	srv := web.NewWithSessionService(wc, st.history, sessions, canceller, sessionTransport.Coordinator)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "run loop: %v\n", err)
		}
	}()

	url := "http://" + addr
	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s - web UI at %s\n", cfg.model, cfg.baseURL, url)
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
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutErr := httpSrv.Shutdown(shutCtx)
	stop()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		fmt.Fprintln(os.Stderr, "shutdown: run loop did not drain in-flight turn within timeout")
	}
	return shutErr
}

type publisherEnqueuer struct {
	channel.Publisher
	channel.Enqueuer
}
