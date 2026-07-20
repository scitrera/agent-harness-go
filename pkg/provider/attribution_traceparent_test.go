package provider

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestApplyAttributionInjectsTraceparentWhenTracing verifies the annotate-and-link
// wiring: with a propagator installed and an active recording span, the outbound LLM
// request carries a W3C traceparent so the platform gateway nests its LLM span under
// the harness's trace.
func TestApplyAttributionInjectsTraceparentWhenTracing(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "turn")
	defer span.End()

	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1/chat/completions", nil)
	applyAttributionHeaders(ctx, req)

	if req.Header.Get("traceparent") == "" {
		t.Fatal("expected a traceparent header under active tracing")
	}
}

// TestApplyAttributionNoTraceparentWithoutActiveSpan verifies TraceContext writes
// nothing when ctx carries no recording span — so a non-traced run stamps no
// traceparent even with a propagator installed.
func TestApplyAttributionNoTraceparentWithoutActiveSpan(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	req, _ := http.NewRequest(http.MethodPost, "http://llm.local/v1", nil)
	applyAttributionHeaders(context.Background(), req)

	if got := req.Header.Get("traceparent"); got != "" {
		t.Fatalf("no active span should inject no traceparent, got %q", got)
	}
}
