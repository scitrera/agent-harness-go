package telemetry

import (
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// MLflow span-attribute conventions. IMPORTANT: on MLflow's OTLP-HTTP ingestion
// path (mlflow/server/otel_api.py) the mlflow.spanType value is stored VERBATIM into
// the spans.type column (no JSON-decode), and the "Tool Calls" dashboard filters
// span.type = 'TOOL'. So the type values MUST be BARE enum strings ("TOOL"), never
// JSON-quoted ("\"TOOL\""), or the dashboard misses them. spanInputs/spanOutputs, by
// contrast, are read via json.loads at query time, so they carry JSON payloads.
// Verified live 2026-07-19; see saas/DESIGN_sahara_agent_tracing.md.
const (
	mlflowSpanType    = "mlflow.spanType"
	mlflowSpanInputs  = "mlflow.spanInputs"
	mlflowSpanOutputs = "mlflow.spanOutputs"

	mlflowTypeAgent = "AGENT"
	mlflowTypeTool  = "TOOL"
)

// MLflowConventions maps the harness spans onto MLflow's tracing schema. It is the
// default Conventions (the platform backend).
type MLflowConventions struct{}

var _ Conventions = MLflowConventions{}

// Turn types the per-turn root as an agent span.
func (MLflowConventions) Turn(protocol.MessageAddress) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String(mlflowSpanType, mlflowTypeAgent)}
}

// LLM is intentionally empty: when the provider call goes through the platform LLM
// gateway, the gateway emits the authoritative mlflow.spanType="LLM" mirror span as
// a child of the harness's provider-call span (via the injected traceparent), so
// typing it here too would double-count LLM spans. For non-gateway providers it
// stays a structural (UNKNOWN) wrapper.
func (MLflowConventions) LLM(string) []attribute.KeyValue { return nil }

// Tool types the span TOOL (bare, so it feeds the "Tool Calls" dashboard) and
// records the raw JSON args as mlflow.spanInputs when present.
func (MLflowConventions) Tool(inputs json.RawMessage) []attribute.KeyValue {
	kv := []attribute.KeyValue{attribute.String(mlflowSpanType, mlflowTypeTool)}
	if len(inputs) > 0 {
		kv = append(kv, attribute.String(mlflowSpanInputs, string(inputs)))
	}
	return kv
}

// ToolResult records the error flag and the raw JSON result payload as
// mlflow.spanOutputs when present.
func (MLflowConventions) ToolResult(outputs json.RawMessage, isError bool) []attribute.KeyValue {
	kv := []attribute.KeyValue{attribute.Bool("tool.is_error", isError)}
	if len(outputs) > 0 {
		kv = append(kv, attribute.String(mlflowSpanOutputs, string(outputs)))
	}
	return kv
}

// Subagent types a sub-agent span as an agent so nested runs render as agent
// sub-trees.
func (MLflowConventions) Subagent() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String(mlflowSpanType, mlflowTypeAgent)}
}
