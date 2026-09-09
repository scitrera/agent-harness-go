// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package telemetry

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestLinkUpstreamExtractsTraceparent(t *testing.T) {
	const traceID = "0af7651916cd43dd8448eb211c80319c"
	addr := protocol.MessageAddress{
		Telemetry: map[string]json.RawMessage{
			"traceparent": json.RawMessage(`"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"`),
		},
	}
	ctx := LinkUpstream(context.Background(), addr)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected a valid remote span context from traceparent")
	}
	if sc.TraceID().String() != traceID {
		t.Fatalf("trace id = %s, want %s", sc.TraceID().String(), traceID)
	}
}

func TestLinkUpstreamNoTelemetryIsNoop(t *testing.T) {
	ctx := LinkUpstream(context.Background(), protocol.MessageAddress{})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("no telemetry should yield no remote span context")
	}
}

func TestStartAndFinishDoNotPanicWithoutExporter(t *testing.T) {
	// With no TracerProvider configured the spans are no-ops; the helpers must
	// still be safe to call.
	ctx, span := StartTurn(context.Background(), protocol.MessageAddress{ThreadID: "t1"})
	_, llm := StartLLM(ctx, "m")
	var nilErr error
	Finish(llm, &nilErr)
	_, tool := StartTool(ctx, "shell", json.RawMessage(`{"cmd":"ls"}`))
	AnnotateToolResult(tool, json.RawMessage(`{"ok":true}`), false)
	FinishErr(tool, context.Canceled)
	Finish(span, &nilErr)
}
