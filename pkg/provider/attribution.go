package provider

import (
	"context"
	"net/http"
)

// Attribution carries the per-turn identifiers stamped as X-Scitrera-*
// headers on outbound LLM requests. The platform LLM gateway reads them for
// tracing/attribution:
//
//	Workspace → X-Scitrera-Workspace → scitrera.workspace
//	ThreadID  → X-Scitrera-Thread-Id → mlflow.trace.session
//	TaskID    → X-Scitrera-Task-Id   → scitrera.task_id
//
// Only the values that change per turn belong here. Sandbox-static
// attribution (tenant/source/user) is injected by the sidecar from its
// proxy_config projection, not by the harness — so it stays authoritative
// and unspoofable. Empty fields are omitted, so a non-sandbox / non-gateway
// run stamps nothing.
type Attribution struct {
	Workspace string
	ThreadID  string
	TaskID    string
}

type attributionKey struct{}

// WithAttribution returns a context carrying a for stamping onto the next LLM
// request built by a SidecarClient. A zero Attribution is a no-op passthrough
// (keeps callers from having to guard the empty case).
func WithAttribution(ctx context.Context, a Attribution) context.Context {
	if a == (Attribution{}) {
		return ctx
	}
	return context.WithValue(ctx, attributionKey{}, a)
}

func attributionFromContext(ctx context.Context) (Attribution, bool) {
	a, ok := ctx.Value(attributionKey{}).(Attribution)
	return a, ok
}

// applyAttributionHeaders stamps the per-turn X-Scitrera-* headers from the
// context onto req. No attribution in context, or an empty field → no header
// (the gateway simply sees fewer attributes). Never overwrites a header a
// caller already set.
func applyAttributionHeaders(ctx context.Context, req *http.Request) {
	a, ok := attributionFromContext(ctx)
	if !ok {
		return
	}
	setHeaderIfAbsent(req.Header, "X-Scitrera-Workspace", a.Workspace)
	setHeaderIfAbsent(req.Header, "X-Scitrera-Thread-Id", a.ThreadID)
	setHeaderIfAbsent(req.Header, "X-Scitrera-Task-Id", a.TaskID)
}

func setHeaderIfAbsent(h http.Header, name, value string) {
	if value == "" || h.Get(name) != "" {
		return
	}
	h.Set(name, value)
}
