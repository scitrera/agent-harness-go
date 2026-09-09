// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package telemetry

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// attrOf returns the value of key on the recorded span, or an empty KeyValue.
func attrOf(kvs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range kvs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// TestMLflowConventionEmitsBareSpanType is the guard against the encoding gotcha the
// spike surfaced: MLflow's OTLP-HTTP ingestion stores mlflow.spanType VERBATIM into
// the spans.type column, and the Tool Calls dashboard filters span.type = 'TOOL', so
// the emitted OTel attribute value MUST be the bare string "TOOL", never JSON-quoted.
func TestMLflowConventionEmitsBareSpanType(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(otel.GetTracerProvider()) })
	SetConventions(MLflowConventions{})

	ctx, turn := StartTurn(context.Background(), protocol.MessageAddress{TenantID: "acme", ThreadID: "t1"})
	_, tool := StartTool(ctx, "read_file", json.RawMessage(`{"path":"/x.py"}`))
	AnnotateToolResult(tool, json.RawMessage(`{"bytes":123}`), false)
	tool.End()
	turn.End()

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(spans))
	}
	byName := map[string]tracetest.SpanStub{}
	for _, s := range spans {
		byName[s.Name] = s
	}

	turnSpan, ok := byName["agent.turn"]
	if !ok {
		t.Fatal("missing agent.turn span")
	}
	if v, ok := attrOf(turnSpan.Attributes, "mlflow.spanType"); !ok || v.AsString() != "AGENT" {
		t.Fatalf("agent.turn mlflow.spanType = %q (ok=%v), want bare \"AGENT\"", v.AsString(), ok)
	}

	// The tool span is NAMED after the tool (so MLflow's dashboard labels it per-tool),
	// not the static "agent.tool.exec".
	toolSpan, ok := byName["read_file"]
	if !ok {
		t.Fatal("missing tool span named after the tool (read_file)")
	}
	v, ok := attrOf(toolSpan.Attributes, "mlflow.spanType")
	if !ok {
		t.Fatal("tool span missing mlflow.spanType")
	}
	// The whole point: the value is the bare enum, not a JSON-quoted string. A
	// wrong (JSON) encoding would yield the 6-char `"TOOL"`.
	if got := v.AsString(); got != "TOOL" {
		t.Fatalf("mlflow.spanType = %q, want bare \"TOOL\" (no JSON quotes)", got)
	}
	if v, ok := attrOf(toolSpan.Attributes, "mlflow.spanInputs"); !ok || v.AsString() != `{"path":"/x.py"}` {
		t.Fatalf("mlflow.spanInputs = %q (ok=%v), want raw JSON args", v.AsString(), ok)
	}
	if v, ok := attrOf(toolSpan.Attributes, "mlflow.spanOutputs"); !ok || v.AsString() != `{"bytes":123}` {
		t.Fatalf("mlflow.spanOutputs = %q (ok=%v), want raw JSON payload", v.AsString(), ok)
	}
	if v, ok := attrOf(toolSpan.Attributes, "tool.is_error"); !ok || v.AsBool() {
		t.Fatalf("tool.is_error = %v (ok=%v), want false", v.AsBool(), ok)
	}
	if v, ok := attrOf(toolSpan.Attributes, "tool.name"); !ok || v.AsString() != "read_file" {
		t.Fatalf("tool.name = %q (ok=%v), want read_file", v.AsString(), ok)
	}
}

// TestMLflowConventionLeavesLLMUntyped verifies the LLM span carries no
// mlflow.spanType so the gateway's mirror LLM span is not double-counted.
func TestMLflowConventionLeavesLLMUntyped(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	SetConventions(MLflowConventions{})

	_, llm := StartLLM(context.Background(), "accounts/fireworks/models/x")
	llm.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	if _, ok := attrOf(spans[0].Attributes, "mlflow.spanType"); ok {
		t.Fatal("agent.llm.request should NOT carry mlflow.spanType (gateway supplies the LLM span)")
	}
	if v, ok := attrOf(spans[0].Attributes, "llm.model"); !ok || v.AsString() != "accounts/fireworks/models/x" {
		t.Fatalf("llm.model = %q (ok=%v)", v.AsString(), ok)
	}
}

// TestNoConventionsAddsNoBackendAttrs verifies the pluggable seam: swapping to
// NoConventions yields plain OTel spans with only the generic identity attributes.
func TestNoConventionsAddsNoBackendAttrs(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	SetConventions(NoConventions{})
	t.Cleanup(func() { SetConventions(MLflowConventions{}) })

	ctx, turn := StartTurn(context.Background(), protocol.MessageAddress{ThreadID: "t1"})
	_, tool := StartTool(ctx, "shell", json.RawMessage(`{"cmd":"ls"}`))
	tool.End()
	turn.End()

	for _, s := range exp.GetSpans() {
		if _, ok := attrOf(s.Attributes, "mlflow.spanType"); ok {
			t.Fatalf("span %q carried mlflow.spanType under NoConventions", s.Name)
		}
	}
}
