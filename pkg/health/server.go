package health

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

type ReadyFunc func(addr string)

type Server struct {
	addr string
}

func NewServer(addr string) Server {
	return Server{addr: addr}
}

func (s Server) Run(ctx context.Context, ready ReadyFunc) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}` + "\n"))
	})

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen health: %w", err)
	}
	defer ln.Close()

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
	if ready != nil {
		ready(ln.Addr().String())
	}

	errCh := make(chan error, 1)
	go func() {
		if serveErr := server.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown health: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve health: %w", err)
		}
		return nil
	}
}
