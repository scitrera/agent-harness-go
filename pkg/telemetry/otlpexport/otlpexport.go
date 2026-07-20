// Package otlpexport installs a global OpenTelemetry tracer + OTLP/HTTP exporter so
// the harness's pkg/telemetry spans (turn/tool/LLM) are exported to MLflow. It is a
// SEPARATE package from pkg/telemetry on purpose: importing the span helpers stays
// lightweight (OTel API only, no-op without a provider), and only a main that calls
// Init links the OTel SDK + exporter. Both the oss reference CLI and the production
// sahara main call Init — otherwise spans are created but never exported.
//
// See saas/DESIGN_sahara_agent_tracing.md.
package otlpexport

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

// Init installs a global TracerProvider (batch OTLP/HTTP exporter) + W3C propagator +
// the selected backend convention when SAHARA_TRACING_ENABLED is set and an OTLP
// endpoint is configured. Otherwise it is a no-op (spans stay no-op, no propagator
// installed) and returns a no-op shutdown. The returned shutdown (always non-nil)
// flushes + stops the exporter — call it before process exit.
//
// Endpoint resolution (first non-empty): SAHARA_TRACING_OTLP_ENDPOINT,
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, OTEL_EXPORTER_OTLP_ENDPOINT. A path-less URL gets
// MLflow's /v1/traces appended. In the platform the endpoint is a sidecar-rewritten
// alias (otlp.local) that reaches the llm-gateway /v1/traces route with authoritative
// tenant attribution stamped; SAHARA_TRACING_EXPERIMENT_ID lets a standalone run stamp
// the experiment id itself. SAHARA_TRACING_CONVENTION selects the attribute vocabulary
// (default MLflow).
func Init(ctx context.Context) (func(context.Context) error, error) {
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
	if hdrs := exportHeaders(); len(hdrs) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(hdrs))
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return noop, fmt.Errorf("otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", getenvOr("SAHARA_TRACING_SERVICE_NAME", "sahara")),
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
	telemetry.SetConventions(telemetry.SelectConventions(os.Getenv("SAHARA_TRACING_CONVENTION")))
	return tp.Shutdown, nil
}

// exportHeaders builds the static OTLP export headers. SAHARA_TRACING_EXPERIMENT_ID
// stamps MLflow's experiment id for standalone runs; in the platform the sidecar
// stamps the per-tenant name header, so this stays empty there.
func exportHeaders() map[string]string {
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

func getenvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
