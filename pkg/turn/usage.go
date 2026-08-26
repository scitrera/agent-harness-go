package turn

import (
	"encoding/json"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// turnUsage accumulates token accounting across the (possibly multi-call) tool
// loop of one turn, so the finalized assistant message carries a single per-turn
// usage record under meta[compaction.MetaUsage] — the authoritative signal trace
// export reads for token/cost analysis and training-data curation.
type turnUsage struct {
	promptTokens             int
	completionTokens         int
	totalTokens              int
	cachedInputTokens        int
	cacheCreationInputTokens int
	calls                    int
	model                    string
}

// add folds one provider call's usage into the running total. model is the model
// that served the call; the last non-empty one wins (it produced the final
// assistant message). A zero Usage still counts as a call.
func (t *turnUsage) add(u provider.Usage, model string) {
	t.promptTokens += u.PromptTokens
	t.completionTokens += u.CompletionTokens
	t.totalTokens += u.TotalTokens
	t.cachedInputTokens += u.CachedInputTokens
	t.cacheCreationInputTokens += u.CacheCreationInputTokens
	t.calls++
	if model != "" {
		t.model = model
	}
}

// stamp writes the accumulated usage onto the assistant message's meta. It is a
// no-op when no provider call was recorded, so an empty/failed turn carries no
// usage record. Existing meta keys are preserved.
func (t *turnUsage) stamp(msg *protocol.ChatMessage) {
	if t == nil || t.calls == 0 {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"model":                       t.model,
		"prompt_tokens":               t.promptTokens,
		"completion_tokens":           t.completionTokens,
		"total_tokens":                t.totalTokens,
		"cached_input_tokens":         t.cachedInputTokens,
		"cache_creation_input_tokens": t.cacheCreationInputTokens,
		"calls":                       t.calls,
	})
	if err != nil {
		slog.Warn("usage: marshal turn usage", slog.Any("err", err))
		return
	}
	if msg.Meta == nil {
		msg.Meta = map[string]json.RawMessage{}
	}
	msg.Meta[compaction.MetaUsage] = payload
}
