// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// stickyModel returns the model pinned for a workspace/thread via /model
// <name>, or "".
func (r *Runner) stickyModel(addr protocol.MessageAddress) string {
	if addr.ThreadID == "" {
		return ""
	}
	r.threadModelsMu.Lock()
	defer r.threadModelsMu.Unlock()
	return r.threadModels[modelThreadKey(addr)]
}

// setStickyModel pins (or clears, when name == "") a workspace/thread's model.
func (r *Runner) setStickyModel(addr protocol.MessageAddress, name string) {
	if addr.ThreadID == "" {
		return
	}
	key := modelThreadKey(addr)
	r.threadModelsMu.Lock()
	defer r.threadModelsMu.Unlock()
	if name == "" {
		delete(r.threadModels, key)
		return
	}
	r.threadModels[key] = name
}

func (r *Runner) hydrateStickyModel(ctx context.Context, addr protocol.MessageAddress) error {
	if r.executionLedger == nil || addr.WorkspaceID == "" || addr.ThreadID == "" {
		return nil
	}
	name, err := r.executionLedger.PinnedModel(ctx, addr)
	if err != nil {
		return err
	}
	if name != "" {
		if r.modelRegistry != nil {
			if _, exists := r.modelRegistry.Get(name); !exists {
				return nil
			}
		}
		r.setStickyModel(addr, name)
	}
	return nil
}

// activeModelName reports the thread's currently-selected model for display: the
// pinned model if set, else the configured default.
func (r *Runner) activeModelName(addr protocol.MessageAddress) string {
	if sticky := r.stickyModel(addr); sticky != "" {
		return sticky
	}
	return r.model
}

// ActiveModelName reports the model selected for threadID.
func (r *Runner) ActiveModelName(threadID string) string {
	return r.activeModelName(protocol.MessageAddress{WorkspaceID: r.defaultWorkspaceID, ThreadID: threadID})
}

// runModelCommand handles /model: bare or "list" lists the available models;
// "<name>" (or "switch <name>") pins the thread's model. With no registry, only
// the single configured model exists and switching is unavailable.
func (r *Runner) runModelCommand(ctx context.Context, addr protocol.MessageAddress, args string) (protocol.ChatMessage, error) {
	fields := strings.Fields(strings.TrimSpace(args))
	if len(fields) == 0 || fields[0] == "list" {
		return r.emitReply(ctx, addr, r.modelListText(addr))
	}
	name := fields[0]
	if fields[0] == "switch" && len(fields) > 1 {
		name = fields[1]
	}
	if r.modelRegistry == nil {
		return r.emitReply(ctx, addr, fmt.Sprintf("Model switching is unavailable; the active model is %q.", r.model))
	}
	if _, ok := r.modelRegistry.Get(name); !ok {
		return r.emitReply(ctx, addr, fmt.Sprintf("Unknown model: %q.\n\n%s", name, r.modelListText(addr)))
	}
	if r.executionLedger != nil {
		if err := r.executionLedger.PinModel(ctx, addr, name); err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("persist model pin: %w", err)
		}
	}
	r.setStickyModel(addr, name)
	return r.emitReply(ctx, addr, fmt.Sprintf("Switched to model %q for this thread.", name))
}

// modelListText renders the available models with the active one marked. Without
// a registry it reports the single configured model.
func (r *Runner) modelListText(addr protocol.MessageAddress) string {
	active := r.activeModelName(addr)
	if r.modelRegistry == nil {
		return fmt.Sprintf("Active model: %s\n(no model registry configured — switching is unavailable)", active)
	}
	var b strings.Builder
	b.WriteString("Available models:")
	for _, m := range r.modelRegistry.List() {
		marker := "  "
		if m.Name == active {
			marker = "* " // active
		}
		b.WriteString("\n  ")
		b.WriteString(marker)
		b.WriteString(m.Name)
		if tags := modelTags(m); tags != "" {
			b.WriteString(" (")
			b.WriteString(tags)
			b.WriteString(")")
		}
	}
	_, _ = fmt.Fprintf(&b, "\n\nActive: %s. Switch with /model MODEL_NAME.", active)
	return b.String()
}

func modelThreadKey(addr protocol.MessageAddress) string {
	return strconv.Itoa(len(addr.WorkspaceID)) + ":" + addr.WorkspaceID + addr.ThreadID
}

// modelTags renders a short capability/tier hint for the model list.
func modelTags(m modelpkg.Model) string {
	var tags []string
	if m.Capabilities.Vision {
		tags = append(tags, "vision")
	}
	if m.Capabilities.Audio {
		tags = append(tags, "audio")
	}
	if m.Tier != "" {
		tags = append(tags, m.Tier)
	}
	if m.Reasoning.DefaultEffort != "" {
		tags = append(tags, "reasoning="+m.Reasoning.DefaultEffort)
	}
	return strings.Join(tags, ", ")
}
