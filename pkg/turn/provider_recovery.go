package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	// maxOverflowRetries bounds reactive context-overflow recovery: on a
	// classified context_overflow error, the loop drops the oldest history and
	// rebuilds, up to this many times before surfacing the error.
	maxOverflowRetries = 2
)

// callWithRecovery builds the context and invokes the provider with the layered
// failure-recovery safety net. Every recovery path acts at the provider-call
// boundary — before any assistant message or tool result is appended — so it is
// side-effect-safe: nothing is re-executed, whether the failure hits the first
// model call or a call after several tool iterations.
//
//  1. context_overflow → drop oldest history and retry the SAME model (up to
//     maxOverflowRetries); the request was rejected before any tokens streamed,
//     so no partial output is duplicated.
//  2. transient failure (rate_limit/server/overloaded/timeout/network) →
//     context-aware backoff, then retry the SAME model, up to
//     maxTransientRetries.
//  3. otherwise (incl. overflow/transient budgets exhausted, or an unknown
//     kind) → ask the Selector for a different model and retry. auth/bad_request
//     are terminal and never switch models. oss's CapabilityDefault declines the
//     fallback, so with no distribution Selector this surfaces the error.
//
// It returns the model that produced the response (possibly switched from the
// input model by a fallback) so the caller can keep using it for the rest of the
// turn.
func (r *Runner) callWithRecovery(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, required modelpkg.Capabilities, history []protocol.ChatMessage, bootstrap []bootstrap.File, injected []protocol.ChatMessage, model string, streamer *turnStreamer, tt turnTools) (provider.ChatResponse, string, error) {
	overflowTrim := 0
	transientRetries := 0
	var modelAttempts []modelpkg.Attempt
	for {
		// Budget compaction to the model we're about to call: on escalation to a
		// smaller-context model, its window (not the orchestrator's) bounds history,
		// so the assembler trims to fit instead of overflowing the target.
		buildCtx := ctx
		if budget := r.modelContextBudget(model); budget > 0 {
			buildCtx = compaction.WithContextBudget(ctx, budget)
		}
		contextMessages, err := r.ctxMgr.Build(buildCtx, bootstrap, trimOldest(history, overflowTrim))
		if err != nil {
			return provider.ChatResponse{}, model, fmt.Errorf("assemble context: %w", err)
		}
		messages := mergeInjected(contextMessages, injected)
		// Resolve provider-undeliverable attachments (vfs_ref-only image/file parts)
		// on the outgoing request only — never persisted history. A resolver error
		// is non-fatal: log and send what we have rather than failing the turn over
		// an attachment (the model still gets the text).
		if resolved, rerr := r.attachments.Resolve(ctx, addr, messages); rerr != nil {
			slog.WarnContext(ctx, "attachment resolution failed; sending request without resolved attachments", slog.Any("err", rerr))
		} else {
			messages = resolved
		}
		reasoningEffort, _ := r.effectiveReasoningEffort(addr, model)
		req := provider.ChatRequest{
			Model: model, Messages: messages, Tools: tt.specs,
			ReasoningEffort: reasoningEffort,
		}
		response, err := r.invokeProvider(ctx, addr, req, streamer)
		if err == nil {
			return response, model, nil
		}
		// A cancelled context aborts the turn — never a recoverable provider failure.
		if ctx.Err() != nil {
			return provider.ChatResponse{}, model, err
		}

		var pe *provider.ProviderError
		isProviderErr := errors.As(err, &pe)

		// 1) context overflow: trim oldest history, retry the same model.
		if isContextOverflow(err) && overflowTrim < maxOverflowRetries && len(history) > 1 {
			overflowTrim++
			slog.WarnContext(ctx, "context overflow; compacting and retrying",
				slog.String("model", model), slog.Int("attempt", overflowTrim))
			r.announceRetry(ctx, addr, hooks.TurnEvent{
				Phase: hooks.PhaseProviderRetry, Addr: addr, Model: model, Err: err,
				Attempt: overflowTrim, Strategy: hooks.RetryTrimContext, FailureKind: fallbackReason(err),
			})
			continue
		}
		// 2) transient: context-aware backoff, retry the same model.
		if isProviderErr && pe.Retryable() && transientRetries < r.maxTransientRetries {
			transientRetries++
			wait := r.retryBackoff(transientRetries)
			slog.WarnContext(ctx, "transient provider failure; backing off and retrying",
				slog.String("model", model), slog.String("kind", string(pe.Kind)),
				slog.Int("attempt", transientRetries), slog.Int64("retry_in_ms", wait.Milliseconds()))
			// Announce BEFORE sleeping: the whole point is that the user sees the
			// wait while it is happening. Announcing after it would describe a
			// pause that has already ended.
			r.announceRetry(ctx, addr, hooks.TurnEvent{
				Phase: hooks.PhaseProviderRetry, Addr: addr, Model: model, Err: err,
				Attempt: transientRetries, RetryIn: wait, Strategy: hooks.RetryBackoff,
				FailureKind: string(pe.Kind),
			})
			if serr := r.backoffSleep(ctx, transientRetries); serr != nil {
				return provider.ChatResponse{}, model, serr
			}
			continue
		}
		// 3) terminal kinds never switch models.
		if isProviderErr && (pe.Kind == provider.FailureAuth || pe.Kind == provider.FailureBadRequest) {
			return provider.ChatResponse{}, model, fmt.Errorf("provider chat: %w", err)
		}
		// 4) cross-model fallback via the Selector seam (opt-in; CapabilityDefault declines).
		next, ok := r.nextFallbackModel(ctx, addr, user, required, model, &modelAttempts, err)
		if !ok {
			return provider.ChatResponse{}, model, fmt.Errorf("provider chat: %w", err)
		}
		slog.WarnContext(ctx, "provider failure; falling back to a different model",
			slog.String("from", model), slog.String("to", next), slog.String("reason", fallbackReason(err)), slog.Any("err", err))
		r.announceRetry(ctx, addr, hooks.TurnEvent{
			Phase: hooks.PhaseProviderRetry, Addr: addr, Model: model, Err: err,
			Attempt: len(modelAttempts), Strategy: hooks.RetryFallbackModel,
			FailureKind: fallbackReason(err), NextModel: next,
		})
		model = next
		// Fresh per-model recovery budgets (a new model may have a larger context
		// window and its own transient behavior).
		overflowTrim = 0
		transientRetries = 0
	}
}

// DynamicProviderRetry is the dynamic-part kind carrying a provider retry to the
// user's channel. A retry the user cannot see is a session that appears hung:
// the turn is alive and waiting, but nothing on screen says so or says for how
// long. Renderers should show the wait as a countdown.
const DynamicProviderRetry = "provider_retry"

// providerRetryPartID is stable for the whole turn, so successive retries UPDATE
// one notice rather than stacking a new one per attempt — a five-attempt ladder
// should read as one status line counting up, not five banners.
const providerRetryPartID = "provider-retry"

// ProviderRetryPayload is the dynamic part's body. Milliseconds (not a duration
// string) because the consumer is a countdown timer.
type ProviderRetryPayload struct {
	Attempt     int    `json:"attempt"`
	RetryInMS   int64  `json:"retry_in_ms"`
	Strategy    string `json:"strategy"`
	FailureKind string `json:"failure_kind,omitempty"`
	Model       string `json:"model,omitempty"`
	NextModel   string `json:"next_model,omitempty"`
	Message     string `json:"message"`
}

// announceRetry reports an imminent provider retry to both audiences: the
// TurnObservers (audit, telemetry, command hooks) and the user's channel.
// Best-effort on both — a failed announcement must never turn a recoverable
// provider failure into a failed turn.
func (r *Runner) announceRetry(ctx context.Context, addr protocol.MessageAddress, ev hooks.TurnEvent) {
	r.notifyTurn(ctx, ev)

	emitter, ok := tools.PartEmitterFrom(ctx)
	if !ok || emitter == nil {
		return
	}
	payload := ProviderRetryPayload{
		Attempt:     ev.Attempt,
		RetryInMS:   ev.RetryIn.Milliseconds(),
		Strategy:    string(ev.Strategy),
		FailureKind: ev.FailureKind,
		Model:       ev.Model,
		NextModel:   ev.NextModel,
		Message:     retryMessage(ev),
	}
	part, err := providerRetryPart(payload)
	if err != nil {
		slog.WarnContext(ctx, "build provider retry part failed", slog.Any("err", err))
		return
	}
	if err := emitter.UpsertPart(ctx, part); err != nil {
		slog.WarnContext(ctx, "emit provider retry part failed", slog.Any("err", err))
	}
}

// providerRetryPart wraps the payload in a dynamic part carrying a stable
// top-level id, which is what the PartEmitter keys on to append once and update
// in place thereafter.
func providerRetryPart(payload ProviderRetryPayload) (protocol.ContentPart, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return protocol.ContentPart{}, err
	}
	raw, err := json.Marshal(struct {
		Type        string          `json:"type"`
		Kind        string          `json:"kind"`
		ID          string          `json:"id"`
		Interactive bool            `json:"interactive"`
		Payload     json.RawMessage `json:"payload"`
	}{
		Type:    string(spec.PartDynamic),
		Kind:    DynamicProviderRetry,
		ID:      providerRetryPartID,
		Payload: body,
	})
	if err != nil {
		return protocol.ContentPart{}, err
	}
	return spec.RawPart(raw)
}

// retryMessage is the fallback rendering for a channel that does not know the
// provider_retry kind: every dynamic part should carry text that stands on its
// own, or an unrecognized kind renders as nothing at all.
func retryMessage(ev hooks.TurnEvent) string {
	kind := ev.FailureKind
	if kind == "" {
		kind = "provider error"
	}
	switch ev.Strategy {
	case hooks.RetryFallbackModel:
		return fmt.Sprintf("%s on %s; switching to %s", kind, ev.Model, ev.NextModel)
	case hooks.RetryTrimContext:
		return fmt.Sprintf("context overflow on %s; trimming history and retrying (attempt %d)", ev.Model, ev.Attempt)
	default:
		return fmt.Sprintf("%s on %s; retrying in %ds (attempt %d)",
			kind, ev.Model, int(ev.RetryIn.Round(time.Second)/time.Second), ev.Attempt)
	}
}

// backoffSleep waits r.retryBackoff(attempt), aborting early if ctx is cancelled.
func (r *Runner) backoffSleep(ctx context.Context, attempt int) error {
	d := r.retryBackoff(attempt)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// nextFallbackModel records the just-failed model and asks the Selector for a
// different model to try. It returns ("", false) when no registry/selector is
// wired, the per-turn model-attempt budget is exhausted, or the Selector
// declines ("") / returns the same or an already-failed model.
func (r *Runner) nextFallbackModel(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, required modelpkg.Capabilities, current string, attempts *[]modelpkg.Attempt, lastErr error) (string, bool) {
	if r.modelSelector == nil || r.modelRegistry == nil {
		return "", false
	}
	*attempts = append(*attempts, modelpkg.Attempt{Model: current, Reason: fallbackReason(lastErr)})
	if len(*attempts) >= r.maxModelAttempts {
		return "", false
	}
	next, err := r.modelSelector.SelectModel(ctx, modelpkg.SelectInput{
		Addr:     addr,
		User:     user,
		Required: required,
		Registry: r.modelRegistry,
		Default:  r.model,
		Attempts: *attempts,
	})
	if err != nil {
		slog.WarnContext(ctx, "model fallback selection failed", slog.Any("err", err))
		return "", false
	}
	if next == "" || next == current {
		return "", false
	}
	for _, a := range *attempts {
		if a.Model == next {
			return "", false // selector returned an already-failed model; stop
		}
	}
	return next, true
}

// fallbackReason extracts the provider failure kind as a string (empty if the
// error is not a classified ProviderError).
func fallbackReason(err error) string {
	var pe *provider.ProviderError
	if errors.As(err, &pe) {
		return string(pe.Kind)
	}
	return ""
}

// defaultRetryBackoff is exponential: 250ms * 2^(attempt-1), capped at 8s.
func defaultRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 16 {
		attempt = 16 // guard the shift from overflowing
	}
	d := 250 * time.Millisecond << (attempt - 1)
	if d > 8*time.Second {
		return 8 * time.Second
	}
	return d
}

// invokeProvider issues one provider call, streaming tokens via the streamer
// when the provider supports it and streaming is enabled.
func (r *Runner) invokeProvider(ctx context.Context, addr protocol.MessageAddress, req provider.ChatRequest, streamer *turnStreamer) (resp provider.ChatResponse, err error) {
	ctx, span := telemetry.StartLLM(ctx, req.Model)
	defer telemetry.Finish(span, &err)
	// Log every provider call + its outcome. The LLM request is otherwise opaque
	// in the harness logs, so a failed call (network/auth/timeout) leaves no
	// trace beyond the task fail reason — which is exactly when we most need to
	// know the model, transport, and error.
	attrs := []any{
		slog.String("model", req.Model),
		slog.Int("messages", len(req.Messages)),
		slog.Int("tools", len(req.Tools)),
	}
	// Surface the multimodal breakdown of the *outgoing* request (post-resolution):
	// how many image/file parts the model will actually see and via which carrier.
	// A non-zero "unresolved" here means a part still has no deliverable carrier
	// after the attachment resolver ran — the breakdown that explains a "I don't
	// see the attached image" reply.
	if mm := summarizeMultimodal(req.Messages); mm.any() {
		attrs = append(attrs, slog.Group("multimodal",
			slog.Int("images", mm.images),
			slog.Int("files", mm.files),
			slog.Int("data_uri", mm.dataURI),
			slog.Int("uri", mm.uri),
			slog.Int("vfs_ref", mm.vfsRef),
			slog.Int("unresolved", mm.unresolved),
		))
	}
	slog.InfoContext(ctx, "llm: provider call", attrs...)
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "llm: provider call failed",
				slog.String("model", req.Model),
				slog.Any("err", err),
			)
		}
	}()
	// Per-turn provider selection: a model that references a named provider
	// (multi-provider config/models.yaml) is served by that upstream; a bare model
	// — or any resolution miss/failure — uses the runner's default provider.
	p := r.provider
	if r.providerResolver != nil {
		if rp, ok := r.providerResolver.ProviderForModel(req.Model); ok {
			p = rp
		}
	}
	start := time.Now()
	if sp, ok := p.(StreamingProvider); ok && r.streaming {
		// One stream part per channel per provider call, created lazily on that
		// channel's first token: reasoning models emit the whole trace before the
		// answer, so the reasoning part is appended first and the text part after
		// it — the true content order. Indices are sticky for the call (a model
		// that interleaves the channels appends to the part it already opened)
		// because the returned response carries ONE block per channel, and the
		// turn's finalize dedups the reconstruction against it by (type, text) —
		// splitting a channel across parts would break that match and duplicate
		// the content.
		textIndex, reasoningIndex := -1, -1
		onDelta := func(kind provider.DeltaKind, text string) error {
			if kind == provider.DeltaReasoning {
				if reasoningIndex < 0 {
					idx, derr := streamer.appendReasoningStream(ctx)
					if derr != nil {
						return derr
					}
					reasoningIndex = idx
				}
				return streamer.tokenDelta(ctx, reasoningIndex, text)
			}
			if textIndex < 0 {
				idx, derr := streamer.appendTextStream(ctx)
				if derr != nil {
					return derr
				}
				textIndex = idx
			}
			return streamer.tokenDelta(ctx, textIndex, text)
		}
		resp, err = sp.ChatStream(ctx, req, onDelta)
	} else {
		resp, err = p.Chat(ctx, req)
	}
	// On success, record the served model, token usage, and latency onto the LLM
	// span + the harness log. Usage is best-effort (zero when the provider omits
	// it / the streamed usage chunk wasn't captured). This is also the single
	// choke point a per-call trace recorder hooks (see TurnRecorder).
	if err == nil {
		respModel := resp.Model
		if respModel == "" {
			respModel = req.Model
		}
		latency := time.Since(start)
		plan := provider.BuildPromptCachePlan(req)
		telemetry.RecordLLMCachePlan(span, plan.StablePrefixBytes, plan.StablePromptDigest, plan.DynamicSuffixDigest, plan.ToolSchemaDigest)
		telemetry.RecordLLMResult(span, respModel, resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Usage.TotalTokens, resp.Usage.CachedInputTokens, resp.Usage.CacheCreationInputTokens, latency)
		slog.InfoContext(ctx, "llm: provider result",
			slog.String("model", respModel),
			slog.Int("prompt_tokens", resp.Usage.PromptTokens),
			slog.Int("completion_tokens", resp.Usage.CompletionTokens),
			slog.Int("total_tokens", resp.Usage.TotalTokens),
			slog.Int("cached_input_tokens", resp.Usage.CachedInputTokens),
			slog.Int("cache_creation_input_tokens", resp.Usage.CacheCreationInputTokens),
			slog.String("stable_prompt_digest", plan.StablePromptDigest),
			slog.String("dynamic_suffix_digest", plan.DynamicSuffixDigest),
			slog.String("tool_schema_digest", plan.ToolSchemaDigest),
			slog.Int64("latency_ms", latency.Milliseconds()))
		// Opt-in trace/training capture: record the assembled prompt + response.
		// Best-effort — a recorder failure must never fail the turn.
		if r.recorder != nil {
			rec := LLMCallRecord{
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				Thread:    addr.ThreadID,
				Model:     respModel,
				Messages:  req.Messages,
				Response:  resp.Message,
				Usage:     resp.Usage,
				LatencyMS: latency.Milliseconds(),
			}
			if rerr := r.recorder.RecordLLMCall(ctx, rec); rerr != nil {
				slog.WarnContext(ctx, "trace recorder failed", slog.Any("err", rerr))
			}
		}
	}
	return resp, err
}
