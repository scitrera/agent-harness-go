// Package telemetry instruments the turn loop with OpenTelemetry spans at the
// key points (turn, LLM request, tool execution, sub-agent). It uses only the
// OTel API: with no TracerProvider/exporter configured (the default) every span
// is a no-op, so this is safe and cheap until the platform wires an exporter.
//
// Upstream linkage: the inbound MessageAddress.telemetry may carry a W3C
// traceparent/tracestate (the cross-system OTel plan), which LinkUpstream
// extracts so the agent's spans are children of the originating trace.
package telemetry

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const scopeName = "scitrera.app/agent-harness"

var propagator = propagation.TraceContext{}

func tracer() trace.Tracer { return otel.Tracer(scopeName) }

// LinkUpstream returns a context whose remote span context is extracted from the
// inbound address's telemetry (traceparent/tracestate), so spans started under
// it are children of the upstream trace. When absent, ctx is returned unchanged.
func LinkUpstream(ctx context.Context, addr protocol.MessageAddress) context.Context {
	carrier := propagation.MapCarrier{}
	if v, ok := stringField(addr.Telemetry, "traceparent"); ok {
		carrier["traceparent"] = v
	}
	if v, ok := stringField(addr.Telemetry, "tracestate"); ok {
		carrier["tracestate"] = v
	}
	if len(carrier) == 0 {
		return ctx
	}
	return propagator.Extract(ctx, carrier)
}

func stringField(m map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := m[key]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		return s, true
	}
	return "", false
}

// StartTurn starts the per-turn span (the agent trace root) with generic
// addressing attributes; the active Conventions add the backend's span-type mapping
// (e.g. MLflow AGENT) so it anchors the agent-shaped trace.
func StartTurn(ctx context.Context, addr protocol.MessageAddress) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String("scitrera.tenant", addr.TenantID),
		attribute.String("scitrera.workspace", addr.WorkspaceID),
		attribute.String("scitrera.thread", addr.ThreadID),
		attribute.String("scitrera.task", addr.TaskID),
	}
	attrs = append(attrs, active.Turn(addr)...)
	return tracer().Start(ctx, "agent.turn", trace.WithAttributes(attrs...))
}

// StartLLM starts a span around one provider request (generic llm.model attribute).
// Whether it carries a backend span-type is the convention's call — the MLflow
// convention leaves it untyped so the platform gateway can supply the authoritative
// LLM mirror span (linked via the injected traceparent) without double-counting.
func StartLLM(ctx context.Context, model string) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{attribute.String("llm.model", model)}
	attrs = append(attrs, active.LLM(model)...)
	return tracer().Start(ctx, "agent.llm.request", trace.WithAttributes(attrs...))
}

// StartTool starts a span around one tool execution (generic tool.name attribute);
// the convention adds the backend span-type + inputs mapping (raw JSON args). Pair
// with AnnotateToolResult at the finish site.
func StartTool(ctx context.Context, name string, inputs json.RawMessage) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{attribute.String("tool.name", name)}
	attrs = append(attrs, active.Tool(inputs)...)
	return tracer().Start(ctx, "agent.tool.exec", trace.WithAttributes(attrs...))
}

// AnnotateToolResult records the tool's output payload + error flag on its span via
// the active convention (e.g. MLflow spanOutputs + tool.is_error). Safe on a
// nil/empty payload. Call before Finish/FinishErr.
func AnnotateToolResult(span trace.Span, outputs json.RawMessage, isError bool) {
	if kv := active.ToolResult(outputs, isError); len(kv) > 0 {
		span.SetAttributes(kv...)
	}
}

// StartSubagent starts a span around an in-process sub-agent run (generic depth
// attribute); the convention adds any backend span-type mapping.
func StartSubagent(ctx context.Context, depth int) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{attribute.Int("subagent.depth", depth)}
	attrs = append(attrs, active.Subagent()...)
	return tracer().Start(ctx, "agent.subagent", trace.WithAttributes(attrs...))
}

// Finish ends a span, recording an error+status when *err is non-nil. Intended
// for `defer Finish(span, &err)` with a named error return.
func Finish(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
	span.End()
}

// FinishErr ends a span, recording an error+status when err is non-nil. For
// call sites with a plain (non-pointer) error in scope.
func FinishErr(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
