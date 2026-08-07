package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	runner, _, err := buildRunner(cfg, st, wc, nil, nil)
	if err != nil {
		return err
	}
	// The web server still takes the concrete filesystem types, so it keeps
	// local transcripts even when MemoryLayer is configured.
	sessions, err := threadindex.NewIndex(cfg.stateDir, time.Now)
	if err != nil {
		return fmt.Errorf("sessions: %w", err)
	}
	canceller := turncancel.New()
	rt, err := runtime.NewRunner(wc, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	srv := web.New(wc, st.files, sessions, canceller)
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
