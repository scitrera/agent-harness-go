package turn

import (
	"context"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// resolveCommand intercepts an OpenClaw-style slash command in the user message.
// It returns:
//
//   - rewritten: the (possibly text-replaced) user message to feed the turn. For
//     a workspace command the text is replaced by the expanded prompt template.
//   - reply + done=true: a synthesized assistant reply when a built-in fully
//     handles the turn (the model is not called).
//   - model: a per-turn model override from a workspace command's frontmatter.
//
// When the line is not a command, rewritten == user and done == false.
func (r *Runner) resolveCommand(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (rewritten protocol.ChatMessage, reply protocol.ChatMessage, done bool, model string, allowedTools []string, err error) {
	name, args, ok := commands.Parse(textOf(user))
	if !ok {
		return user, protocol.ChatMessage{}, false, "", nil, nil
	}
	// Reserved built-ins win over workspace files and never reach the model.
	if commands.IsReserved(name) {
		reply, err = r.runBuiltin(ctx, addr, user, name, args)
		return user, reply, true, "", nil, err
	}
	if cmd, found := r.commands.Lookup(name); found {
		expanded := commands.Expand(cmd.Body, args)
		return replaceUserText(user, expanded), protocol.ChatMessage{}, false, cmd.Model, cmd.AllowedTools, nil
	}
	// Unknown command: helpful reply, model not called.
	reply, err = r.emitReply(ctx, addr, fmt.Sprintf("Unknown command: /%s. Type /help to list available commands.", name))
	return user, reply, true, "", nil, err
}

// runBuiltin handles a reserved built-in command, returning the synthesized
// assistant reply (also published via the streamer for Aether egress).
func (r *Runner) runBuiltin(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, name, args string) (protocol.ChatMessage, error) {
	switch commands.CanonicalKey(name) {
	case "help", "commands":
		return r.emitReply(ctx, addr, r.helpText())
	case "clear":
		var err error
		if scoped, ok := r.store.(harness.WorkspaceHistoryStore); ok && addr.WorkspaceID != "" {
			err = scoped.SaveWorkspaceHistory(ctx, addr.WorkspaceID, addr.ThreadID, nil)
		} else {
			err = r.store.SaveHistory(ctx, addr.ThreadID, nil)
		}
		if err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("clear history: %w", err)
		}
		return r.emitReply(ctx, addr, "Thread history cleared.")
	case "model", "models":
		return r.runModelCommand(ctx, addr, args)
	case "schedules", "runs":
		if r.scheduledOperations == nil {
			return r.emitReply(ctx, addr, "Scheduled operations are unavailable: this runtime is not connected to Aether WorkflowEngine.")
		}
		text, err := r.scheduledOperations.RunScheduledOperationsCommand(ctx, addr, user, commands.CanonicalKey(name), args)
		if err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("/%s: %w", commands.CanonicalKey(name), err)
		}
		return r.emitReply(ctx, addr, text)
	case "refinements":
		if r.refinementAudit == nil {
			return r.emitReply(ctx, addr, "Refinement audit browsing is unavailable: this runtime has no refinement authority configured.")
		}
		text, err := r.refinementAudit.RunRefinementAuditCommand(ctx, addr, user, args)
		if err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("/refinements: %w", err)
		}
		return r.emitReply(ctx, addr, text)
	default:
		return r.emitReply(ctx, addr, "Unknown command.")
	}
}

// helpText renders the reserved built-ins plus discovered workspace commands.
func (r *Runner) helpText() string {
	var b strings.Builder
	b.WriteString("Available commands:\n")
	b.WriteString("  /help          List available commands.\n")
	b.WriteString("  /commands      List available commands.\n")
	b.WriteString("  /clear         Clear this thread's history.\n")
	b.WriteString("  /model         List models, or /model MODEL_NAME to switch.")
	b.WriteString("\n  /models        List models (alias for /model).")
	if r.scheduledOperations != nil {
		b.WriteString("\n  /schedules     Inspect authoritative scheduled-turn definitions.")
		b.WriteString("\n  /runs          Inspect runs; use /runs --help for filters and cursors.")
	}
	if r.refinementAudit != nil {
		b.WriteString("\n  /refinements   Browse the bounded authoritative refinement audit.")
	}
	for _, c := range r.commands.List() {
		b.WriteString("\n  /")
		b.WriteString(c.Name)
		if c.ArgumentHint != "" {
			b.WriteString(" ")
			b.WriteString(c.ArgumentHint)
		}
		if c.Description != "" {
			b.WriteString("  — ")
			b.WriteString(c.Description)
		}
	}
	return b.String()
}

// emitReply builds an assistant text message, publishes the start+finalize
// lifecycle (so Aether egress sees it), and returns it. Built-in replies are not
// persisted to history or committed to memory: they are meta, not conversation.
func (r *Runner) emitReply(ctx context.Context, addr protocol.MessageAddress, text string) (protocol.ChatMessage, error) {
	part, err := protocol.NewTextPart(text)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	id := streamMessageID(addr)
	msg := protocol.ChatMessage{
		SchemaVersion: "1.0",
		ID:            id,
		Role:          protocol.RoleAssistant,
		Addr:          addr,
		Content:       []protocol.ContentPart{part},
	}
	streamer := newTurnStreamer(r.publisher, addr, id, r.now, r.streamFlush)
	if err := streamer.start(ctx); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_started: %w", err)
	}
	finalized, err := streamer.finalize(ctx, msg)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_finalized: %w", err)
	}
	return finalized, nil
}

// replaceUserText returns a copy of msg whose text content is replaced by text,
// preserving non-text parts (e.g. attached files) and message identity (ID,
// role, addr, meta) so OBO authority and history threading are unaffected.
func replaceUserText(msg protocol.ChatMessage, text string) protocol.ChatMessage {
	out := msg
	parts := make([]protocol.ContentPart, 0, len(msg.Content)+1)
	if tp, err := protocol.NewTextPart(text); err == nil {
		parts = append(parts, tp)
	}
	for _, p := range msg.Content {
		if _, ok := p.AsText(); ok {
			continue
		}
		parts = append(parts, p)
	}
	out.Content = parts
	return out
}
