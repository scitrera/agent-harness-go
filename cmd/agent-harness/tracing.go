package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/scitrera/agent-harness-go/pkg/telemetry"
)

// initTracing installs a global OTel TracerProvider + W3C propagator so the
// harness's already-instrumented turn/tool spans (pkg/telemetry) export to MLflow
// over OTLP/HTTP. Gated on SAHARA_TRACING_ENABLED + an OTLP endpoint; when either
// is absent it is a no-op (spans stay no-op, no propagator installed) and returns a
// no-op shutdown. The returned shutdown (always non-nil) flushes+stops the batch
// exporter — call it before process exit.
//
// Endpoint resolution (first non-empty): SAHARA_TRACING_OTLP_ENDPOINT,
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, OTEL_EXPORTER_OTLP_ENDPOINT. A path-less URL
// gets MLflow's /v1/traces appended. In the platform the endpoint is the sidecar's
// local OTLP receiver, which forwards to the llm-gateway /v1/traces route and stamps
// x-mlflow-experiment-id authoritatively; SAHARA_TRACING_EXPERIMENT_ID lets a
// standalone harness stamp it itself. See saas/DESIGN_sahara_agent_tracing.md.
func initTracing(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if !envBool("SAHARA_TRACING_ENABLED") {
		return noop, nil
	}
	endpoint := firstNonEmpty(
		os.Getenv("SAHARA_TRACING_OTLP_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	)
	if endpoint == "" {
		return noop, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return noop, fmt.Errorf("parse tracing endpoint %q: %w", endpoint, err)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(u.String())}
	if hdrs := tracingHeaders(); len(hdrs) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(hdrs))
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return noop, fmt.Errorf("otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", env("SAHARA_TRACING_SERVICE_NAME", "sahara")),
	))
	if err != nil {
		res = resource.Default()
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// Select the backend attribute convention (default MLflow). Pluggable so a sink
	// with different conventions can be swapped without touching the turn loop.
	telemetry.SetConventions(telemetry.SelectConventions(os.Getenv("SAHARA_TRACING_CONVENTION")))
	return tp.Shutdown, nil
}

// tracingHeaders builds the static OTLP export headers. SAHARA_TRACING_EXPERIMENT_ID
// stamps MLflow's required experiment id for standalone runs; in the platform the
// sidecar receiver stamps it per-tenant, so this stays empty there.
func tracingHeaders() map[string]string {
	h := map[string]string{}
	if v := os.Getenv("SAHARA_TRACING_EXPERIMENT_ID"); v != "" {
		h["x-mlflow-experiment-id"] = v
	}
	return h
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
