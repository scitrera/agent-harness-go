package turn

import (
	"context"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// AttachmentResolver rewrites multimodal content parts whose only carrier is one
// the model provider cannot fetch on its own — chiefly vfs_ref-only image/file
// parts — into a model-deliverable form (an inline data_uri, or a fetchable uri).
//
// It runs on the *assembled request* immediately before the provider call, never
// on persisted history: a resolved part may carry inlined bytes or a short-lived
// presigned URL that must not be written back to the store. Resolve must not
// mutate its input; it returns the (possibly rewritten) messages to send.
//
// The per-turn OBO authority is carried on ctx (set by the runner via
// tools.WithMemoryAuthority); a resolver that fetches from an authority-scoped
// service (e.g. data-connectors) reads it there. addr supplies the tenant +
// workspace the attachment belongs to.
//
// The default wiring is NoopAttachmentResolver, which resolves nothing and logs
// the parts it leaves undeliverable. The sahara distribution supplies a
// data-connectors-backed resolver that mints a download URL, materializes the
// blob into the workspace, and inlines it.
type AttachmentResolver interface {
	Resolve(ctx context.Context, addr protocol.MessageAddress, messages []protocol.ChatMessage) ([]protocol.ChatMessage, error)
}

// NoopAttachmentResolver is the default resolver: it returns the messages
// unchanged and logs any image/file part that has no model-deliverable carrier
// (vfs_ref-only or empty). The harness core ships no VFS resolver, so this is
// the canonical "attachment dropped" log — it makes a missing attachment visible
// even when the distribution wires no real resolver.
type NoopAttachmentResolver struct{}

func (NoopAttachmentResolver) Resolve(ctx context.Context, _ protocol.MessageAddress, messages []protocol.ChatMessage) ([]protocol.ChatMessage, error) {
	for _, m := range messages {
		for _, p := range m.Content {
			switch p.Type() {
			case protocol.ContentImage:
				if img, ok := p.AsImage(); ok && img.DataURI == "" && img.URI == "" && img.VFSRef != "" {
					slog.WarnContext(ctx, "attachment: image dropped — no resolver to fetch its vfs_ref (it will not reach the model)",
						slog.String("vfs_ref", img.VFSRef),
						slog.String("mime", img.Mime),
					)
				}
			case protocol.ContentFile:
				if f, ok := p.AsFile(); ok && f.URI == "" && f.VFSRef != "" {
					slog.WarnContext(ctx, "attachment: file dropped — no resolver to fetch its vfs_ref (it will not reach the model)",
						slog.String("vfs_ref", f.VFSRef),
						slog.String("file_name", f.FileName),
					)
				}
			}
		}
	}
	return messages, nil
}
