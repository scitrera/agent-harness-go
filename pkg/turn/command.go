package turn

import (
	"context"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/commands"
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
		reply, err = r.runBuiltin(ctx, addr, name)
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
func (r *Runner) runBuiltin(ctx context.Context, addr protocol.MessageAddress, name string) (protocol.ChatMessage, error) {
	switch commands.CanonicalKey(name) {
	case "help", "commands":
		return r.emitReply(ctx, addr, r.helpText())
	case "clear":
		if err := r.store.SaveHistory(ctx, addr.ThreadID, nil); err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("clear history: %w", err)
		}
		return r.emitReply(ctx, addr, "Thread history cleared.")
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
	b.WriteString("  /clear         Clear this thread's history.")
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
	streamer := newTurnStreamer(r.publisher, addr, id, r.now)
	if err := streamer.start(ctx); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_started: %w", err)
	}
	if err := streamer.finalize(ctx, msg); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_finalized: %w", err)
	}
	return msg, nil
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
