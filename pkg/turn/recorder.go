// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// LLMCallRecord captures one provider call for trace export / training data: the
// assembled request the model actually saw (post context-assembly, compaction,
// and attachment resolution) plus its response, token usage, and latency.
type LLMCallRecord struct {
	Timestamp string                 `json:"ts,omitempty"`
	Thread    string                 `json:"thread,omitempty"`
	Model     string                 `json:"model"`
	Messages  []protocol.ChatMessage `json:"messages"`
	Response  protocol.ChatMessage   `json:"response"`
	Usage     provider.Usage         `json:"usage"`
	LatencyMS int64                  `json:"latency_ms"`
}

// TurnRecorder receives one record per successful provider call — the seam for
// trace / training-data capture. Implementations must be safe for concurrent use
// (sub-agents run turns in parallel) and should be cheap/non-blocking.
//
// Wiring one is OPT-IN: the core leaves it nil so recording is a no-op with zero
// overhead. It captures the FULL assembled prompt on every call, which is only
// worth its cost when explicitly collecting data — so the reference CLI gates it
// behind a --record flag and distributions wire it only when a pipeline consumes it.
type TurnRecorder interface {
	RecordLLMCall(ctx context.Context, rec LLMCallRecord) error
}
