package turn

import (
	"context"
	"fmt"
	"strings"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// stickyModel returns the model pinned for a thread via /model <name>, or "".
func (r *Runner) stickyModel(threadID string) string {
	if threadID == "" {
		return ""
	}
	r.threadModelsMu.Lock()
	defer r.threadModelsMu.Unlock()
	return r.threadModels[threadID]
}

// setStickyModel pins (or clears, when name == "") a thread's model.
func (r *Runner) setStickyModel(threadID, name string) {
	if threadID == "" {
		return
	}
	r.threadModelsMu.Lock()
	defer r.threadModelsMu.Unlock()
	if name == "" {
		delete(r.threadModels, threadID)
		return
	}
	r.threadModels[threadID] = name
}

// activeModelName reports the thread's currently-selected model for display: the
// pinned model if set, else the configured default.
func (r *Runner) activeModelName(threadID string) string {
	if sticky := r.stickyModel(threadID); sticky != "" {
		return sticky
	}
	return r.model
}

// ActiveModelName reports the model selected for threadID.
func (r *Runner) ActiveModelName(threadID string) string {
	return r.activeModelName(threadID)
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
	r.setStickyModel(addr.ThreadID, name)
	return r.emitReply(ctx, addr, fmt.Sprintf("Switched to model %q for this thread.", name))
}

// modelListText renders the available models with the active one marked. Without
// a registry it reports the single configured model.
func (r *Runner) modelListText(addr protocol.MessageAddress) string {
	active := r.activeModelName(addr.ThreadID)
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
	b.WriteString(fmt.Sprintf("\n\nActive: %s. Switch with /model <name>.", active))
	return b.String()
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
	return strings.Join(tags, ", ")
}
