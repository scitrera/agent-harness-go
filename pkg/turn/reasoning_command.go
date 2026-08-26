// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"fmt"
	"strings"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const reasoningProviderDefault = "provider default"

func reasoningThreadKey(addr protocol.MessageAddress, modelName string) string {
	return modelThreadKey(addr) + "\x00" + modelName
}

func (r *Runner) stickyReasoningEffort(addr protocol.MessageAddress, modelName string) string {
	if addr.ThreadID == "" {
		return ""
	}
	r.threadReasoningMu.Lock()
	defer r.threadReasoningMu.Unlock()
	return r.threadReasoning[reasoningThreadKey(addr, modelName)]
}

func (r *Runner) setStickyReasoningEffort(addr protocol.MessageAddress, modelName, effort string) {
	if addr.ThreadID == "" {
		return
	}
	key := reasoningThreadKey(addr, modelName)
	r.threadReasoningMu.Lock()
	defer r.threadReasoningMu.Unlock()
	if effort == "" {
		delete(r.threadReasoning, key)
		return
	}
	r.threadReasoning[key] = effort
}

func (r *Runner) modelReasoningConfig(modelName string) modelpkg.ReasoningConfig {
	if configured, ok := r.modelRegistry.Get(modelName); ok {
		return configured.Reasoning
	}
	return modelpkg.ReasoningConfig{}
}

// effectiveReasoningEffort resolves request policy in descending precedence:
// per-thread+model override, process/user preference, model default, provider
// default. An allowlist applies to all override sources.
func (r *Runner) effectiveReasoningEffort(addr protocol.MessageAddress, modelName string) (string, string) {
	config := r.modelReasoningConfig(modelName)
	if effort := r.stickyReasoningEffort(addr, modelName); effort != "" && config.Allows(effort) {
		return effort, "thread override"
	}
	if r.reasoningEffort != "" && config.Allows(r.reasoningEffort) {
		return r.reasoningEffort, "process preference"
	}
	if effort, err := modelpkg.NormalizeReasoningEffort(config.DefaultEffort); err == nil && effort != "" && config.Allows(effort) {
		return effort, "model default"
	}
	return "", reasoningProviderDefault
}

// ActiveReasoningEffort reports the effective effort for a thread's active
// model. Empty means the request omits the setting and lets the provider decide.
func (r *Runner) ActiveReasoningEffort(threadID string) string {
	addr := protocol.MessageAddress{WorkspaceID: r.defaultWorkspaceID, ThreadID: threadID}
	effort, _ := r.effectiveReasoningEffort(addr, r.activeModelName(addr))
	return effort
}

func (r *Runner) runReasoningCommand(ctx context.Context, addr protocol.MessageAddress, args string) (protocol.ChatMessage, error) {
	modelName := r.activeModelName(addr)
	fields := strings.Fields(strings.TrimSpace(args))
	if len(fields) == 0 || (len(fields) == 1 && strings.EqualFold(fields[0], "list")) {
		return r.emitReply(ctx, addr, r.reasoningStatusText(addr, modelName))
	}
	if len(fields) != 1 {
		return r.emitReply(ctx, addr, "Usage: /reasoning [none|minimal|low|medium|high|xhigh|max|default]")
	}
	if strings.EqualFold(fields[0], "default") || strings.EqualFold(fields[0], "reset") {
		r.setStickyReasoningEffort(addr, modelName, "")
		return r.emitReply(ctx, addr, "Reasoning override cleared.\n\n"+r.reasoningStatusText(addr, modelName))
	}
	effort, err := modelpkg.NormalizeReasoningEffort(fields[0])
	if err != nil || effort == "" {
		if err == nil {
			err = fmt.Errorf("reasoning effort is empty")
		}
		return r.emitReply(ctx, addr, fmt.Sprintf("Invalid reasoning effort: %v.\n\n%s", err, r.reasoningStatusText(addr, modelName)))
	}
	config := r.modelReasoningConfig(modelName)
	if !config.Allows(effort) {
		return r.emitReply(ctx, addr, fmt.Sprintf("Reasoning effort %q is not allowed for model %q.\n\n%s", effort, modelName, r.reasoningStatusText(addr, modelName)))
	}
	r.setStickyReasoningEffort(addr, modelName, effort)
	return r.emitReply(ctx, addr, fmt.Sprintf("Reasoning effort set to %q for model %q in this thread.", effort, modelName))
}

func (r *Runner) reasoningStatusText(addr protocol.MessageAddress, modelName string) string {
	effort, source := r.effectiveReasoningEffort(addr, modelName)
	display := reasoningProviderDefault
	if effort != "" {
		display = effort + " (" + source + ")"
	}
	allowed := r.modelReasoningConfig(modelName).AllowedEfforts
	if len(allowed) == 0 {
		allowed = modelpkg.KnownReasoningEfforts()
	}
	return fmt.Sprintf(
		"Model: %s\nReasoning effort: %s\nAllowed: %s\nSet with /reasoning EFFORT; clear the thread override with /reasoning default.",
		modelName, display, strings.Join(allowed, ", "),
	)
}
