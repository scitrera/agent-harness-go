package turn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
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
		req := provider.ChatRequest{Model: model, Messages: messages, Tools: tt.specs}
		response, err := r.invokeProvider(ctx, req, streamer)
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
			continue
		}
		// 2) transient: context-aware backoff, retry the same model.
		if isProviderErr && pe.Retryable() && transientRetries < r.maxTransientRetries {
			transientRetries++
			slog.WarnContext(ctx, "transient provider failure; backing off and retrying",
				slog.String("model", model), slog.String("kind", string(pe.Kind)), slog.Int("attempt", transientRetries))
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
		model = next
		// Fresh per-model recovery budgets (a new model may have a larger context
		// window and its own transient behavior).
		overflowTrim = 0
		transientRetries = 0
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
func (r *Runner) invokeProvider(ctx context.Context, req provider.ChatRequest, streamer *turnStreamer) (resp provider.ChatResponse, err error) {
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
	if sp, ok := p.(StreamingProvider); ok && r.streaming {
		textIndex := -1
		onDelta := func(text string) error {
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
		return resp, err
	}
	resp, err = p.Chat(ctx, req)
	return resp, err
}
