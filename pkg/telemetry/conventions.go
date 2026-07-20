package telemetry

import (
	"encoding/json"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// Conventions maps the harness's semantic span points (turn, LLM call, tool exec,
// sub-agent) onto a tracing backend's attribute vocabulary, so a backend whose
// conventions differ from MLflow's — e.g. OpenInference or the OTel GenAI semantic
// conventions — can be swapped in without touching the turn loop. Each method
// returns ONLY the backend-specific attributes for that span; the generic identity
// attributes (scitrera.*, llm.model, tool.name, subagent.depth) are always set by
// the telemetry package regardless of the active convention.
//
// Implementations must be safe to call with a nil/zero convention receiver and
// return nil when they have nothing to add.
type Conventions interface {
	// Turn returns backend attributes for the per-turn root span.
	Turn(addr protocol.MessageAddress) []attribute.KeyValue
	// LLM returns backend attributes for a provider-call span.
	LLM(model string) []attribute.KeyValue
	// Tool returns backend attributes for a tool-exec span; inputs is the raw
	// JSON tool arguments (may be empty).
	Tool(inputs json.RawMessage) []attribute.KeyValue
	// ToolResult returns backend attributes recorded at tool finish; outputs is
	// the raw JSON result payload (may be empty).
	ToolResult(outputs json.RawMessage, isError bool) []attribute.KeyValue
	// Subagent returns backend attributes for a sub-agent span.
	Subagent() []attribute.KeyValue
}

// active is the convention applied to every span. Default = MLflow (the platform
// backend). Override once at startup via SetConventions before any span is created.
var active Conventions = MLflowConventions{}

// SetConventions swaps the active convention (no-op on nil). Call once during
// startup, before the first turn — it is not safe to change concurrently with span
// creation.
func SetConventions(c Conventions) {
	if c != nil {
		active = c
	}
}

// SelectConventions maps a name (e.g. from SAHARA_TRACING_CONVENTION) to a built-in
// Conventions implementation. Empty/unknown → MLflow (the platform default);
// "none"/"otel"/"generic" → plain OTel spans with no backend-specific attributes.
func SelectConventions(name string) Conventions {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "none", "otel", "generic":
		return NoConventions{}
	default:
		return MLflowConventions{}
	}
}

// NoConventions adds no backend-specific attributes — plain OTel spans carrying only
// the generic identity attributes. Useful when the trace sink applies its own
// attribute mapping (e.g. an OTel Collector processor) or for a backend consumed via
// raw OTel semantics.
type NoConventions struct{}

func (NoConventions) Turn(protocol.MessageAddress) []attribute.KeyValue     { return nil }
func (NoConventions) LLM(string) []attribute.KeyValue                       { return nil }
func (NoConventions) Tool(json.RawMessage) []attribute.KeyValue             { return nil }
func (NoConventions) ToolResult(json.RawMessage, bool) []attribute.KeyValue { return nil }
func (NoConventions) Subagent() []attribute.KeyValue                        { return nil }
