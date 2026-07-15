package tools

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const presentArtifactToolName = "present_artifact"

// presentedSet remembers which artifact paths each thread has already presented,
// so a repeat present of the same deliverable can be flagged. Keyed by thread id
// then raw path; small (paths are short) and bounded by live threads.
type presentedSet struct {
	mu   sync.Mutex
	seen map[string]map[string]struct{}
}

func newPresentedSet() *presentedSet {
	return &presentedSet{seen: map[string]map[string]struct{}{}}
}

// mark records (thread, path) and reports whether it was new (true) or a repeat
// (false).
func (p *presentedSet) mark(thread, path string) (isNew bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	byPath := p.seen[thread]
	if byPath == nil {
		byPath = map[string]struct{}{}
		p.seen[thread] = byPath
	}
	if _, ok := byPath[path]; ok {
		return false
	}
	byPath[path] = struct{}{}
	return true
}

// defaultArtifactInlineMax: an image at or below this size is inlined as a
// data_uri (no upload round-trip); a larger image, or any non-image file, is
// uploaded via the ArtifactUploader and referenced by vfs_ref.
const defaultArtifactInlineMax = 128 << 10 // 128 KiB

// defaultArtifactMaxBytes caps a single artifact read so a runaway file cannot
// exhaust memory.
const defaultArtifactMaxBytes = 32 << 20 // 32 MiB

// ArtifactUploader uploads agent-produced artifact bytes to a durable,
// frontend-fetchable store and returns an opaque reference (a data-connectors
// vfs_ref) the frontend resolves to a presigned download URL. The harness core
// ships no uploader; the distribution wires one (sahara: data-connectors). A nil
// uploader limits present_artifact to inlining images (non-image files error).
type ArtifactUploader interface {
	Upload(ctx context.Context, addr protocol.MessageAddress, name, mimeType string, data []byte) (vfsRef string, err error)
}

// ArtifactConfig configures the present_artifact tool.
type ArtifactConfig struct {
	// Workspace reads the artifact bytes the agent names. Required.
	Workspace *localtools.Workspace
	// Uploader stores large images + non-image files and returns a vfs_ref. nil →
	// inline images only; non-image files error.
	Uploader ArtifactUploader
	// InlineMaxBytes is the image inline-vs-upload threshold (<=0 → 128 KiB).
	InlineMaxBytes int
	// MaxBytes hard-caps a single artifact read (<=0 → 32 MiB).
	MaxBytes int
	// SessionDir, when set, returns the per-(thread,context) working directory a
	// code session writes relative files into (e.g. sahara's
	// /sahara/sessions/<key>). A RELATIVE artifact path is resolved against it
	// first — so a file a kernel saved with a bare name (plt.savefig('p.png')) is
	// found — falling back to the workspace root. nil (or "" result) → paths
	// resolve against the workspace root only (the prior behavior). The harness
	// stays scheme-agnostic; the distribution owns the path derivation.
	SessionDir func(threadID, context string) string
}

// RegisterArtifact registers present_artifact: the agent names workspace files it
// produced (e.g. a chart it saved) and the harness surfaces them to the user as
// native image/file content parts on the assistant message — bypassing markdown
// image rendering entirely. Small images inline as a data_uri; larger images and
// non-image files upload via the ArtifactUploader and ride as a vfs_ref the
// frontend presigns and fetches.
func RegisterArtifact(reg *Registry, cfg ArtifactConfig) error {
	if cfg.Workspace == nil {
		return fmt.Errorf("%w: artifact workspace required", ErrInvalidTool)
	}
	inlineMax := cfg.InlineMaxBytes
	if inlineMax <= 0 {
		inlineMax = defaultArtifactInlineMax
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultArtifactMaxBytes
	}
	// seen tracks paths already presented on each thread so a repeat present of
	// the same deliverable returns a gentle "already submitted" note. present is a
	// completion signal; re-presenting the same file in a tight refine loop
	// (without terminating) is a known failure mode. The note nudges termination
	// without blocking a legitimate revision (the part is still emitted).
	seen := newPresentedSet()
	err := reg.Register(presentArtifactToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		var args struct {
			Paths   []string `json:"paths"`
			Context string   `json:"context"`
		}
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
		if len(args.Paths) == 0 {
			return errorResult(req, "paths is required: name one or more workspace files to present")
		}
		emitter, hasEmitter := PartEmitterFrom(ctx)
		if !hasEmitter {
			// No streaming surface (e.g. a non-streaming CLI path): the part cannot
			// reach the user, so report rather than silently no-op.
			return errorResult(req, "cannot present artifacts on this turn (no streaming surface)")
		}
		presented := make([]string, 0, len(args.Paths))
		repeats := make([]string, 0)
		for _, p := range args.Paths {
			data, err := readArtifactBytes(ctx, cfg, req.Addr.ThreadID, args.Context, p, int64(maxBytes))
			if err != nil {
				return errorResult(req, fmt.Sprintf("read %s: %v", p, err))
			}
			name := filepath.Base(p)
			mimeType := artifactMime(name, data)
			part, desc, err := buildArtifactPart(ctx, req.Addr, cfg.Uploader, inlineMax, name, mimeType, data)
			if err != nil {
				return errorResult(req, fmt.Sprintf("present %s: %v", p, err))
			}
			if err := emitter.UpsertPart(ctx, part); err != nil {
				return Result{}, fmt.Errorf("emit artifact part: %w", err)
			}
			presented = append(presented, fmt.Sprintf("%s (%s)", name, desc))
			if !seen.mark(req.Addr.ThreadID, p) {
				repeats = append(repeats, name)
			}
		}
		result := map[string]any{"presented": presented}
		if len(repeats) > 0 {
			result["note"] = fmt.Sprintf("You have already presented %s. present_artifact submits a COMPLETED deliverable; "+
				"only re-present a file if you substantively revised it. If every deliverable is final and presented, "+
				"you are done: give a brief summary and stop.", strings.Join(repeats, ", "))
		}
		payload, err := json.Marshal(result)
		if err != nil {
			return Result{}, err
		}
		return NewJSONResult(req.CallID, req.Name, payload)
	}))
	if err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        presentArtifactToolName,
		Description: "Surface a COMPLETED deliverable you saved (e.g. a chart, image, or document) to the user as an inline artifact in your reply. Presenting is a completion signal: finish the file fully first, then present it once — do NOT present drafts, previews, or work-in-progress, and do not re-present a file you have not substantively revised. When every deliverable is final and presented, give a brief summary and stop. A bare relative path (e.g. \"plot.png\") resolves to your code session's working directory — exactly where the python tool saves relative files — so you can present what you just wrote without an absolute path. Images render inline; other files attach as downloads. Use this instead of a markdown image link — a local file path will NOT render. If you saved under a named `context`, pass the same `context` here.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"},"description":"File path(s) to present; a bare name resolves to your code session dir, e.g. [\"plot.png\"]"},"context":{"type":"string","description":"Optional: the code context label the file was saved under (omit for the main session)"}},"required":["paths"]}`),
	})
	return nil
}

// readArtifactBytes reads an artifact's bytes. A RELATIVE path is resolved
// against the (thread,context) code-session dir first — where a kernel writes
// relative files (plt.savefig('p.png')) — then falls back to the workspace root
// (where write_file etc. write); an ABSOLUTE path is read directly. SessionDir is
// the distribution-provided derivation (nil → workspace-root only).
func readArtifactBytes(ctx context.Context, cfg ArtifactConfig, threadID, contextLabel, p string, maxBytes int64) ([]byte, error) {
	if !filepath.IsAbs(p) && cfg.SessionDir != nil {
		if dir := cfg.SessionDir(threadID, contextLabel); dir != "" {
			if b, err := cfg.Workspace.ReadBytes(ctx, dir+"/"+p, maxBytes); err == nil {
				return b, nil
			}
		}
	}
	return cfg.Workspace.ReadBytes(ctx, p, maxBytes)
}

// buildArtifactPart turns artifact bytes into the content part to stream: an
// inline image (data_uri) when small, otherwise an uploaded image/file part
// (vfs_ref). desc is a short human label for the tool-result summary.
func buildArtifactPart(ctx context.Context, addr protocol.MessageAddress, up ArtifactUploader, inlineMax int, name, mimeType string, data []byte) (protocol.ContentPart, string, error) {
	isImage := strings.HasPrefix(mimeType, "image/")
	if isImage && len(data) <= inlineMax {
		part, err := protocol.NewImagePart(protocol.ImagePart{Mime: mimeType, DataURI: artifactDataURI(mimeType, data), AltText: name})
		return part, "image, inline", err
	}
	if up == nil {
		if isImage {
			// No uploader wired: inline the image anyway rather than dropping it.
			part, err := protocol.NewImagePart(protocol.ImagePart{Mime: mimeType, DataURI: artifactDataURI(mimeType, data), AltText: name})
			return part, "image, inline (no uploader)", err
		}
		return protocol.ContentPart{}, "", fmt.Errorf("no artifact uploader configured; cannot present a non-image file")
	}
	vfsRef, err := up.Upload(ctx, addr, name, mimeType, data)
	if err != nil {
		return protocol.ContentPart{}, "", fmt.Errorf("upload: %w", err)
	}
	if isImage {
		part, perr := protocol.NewImagePart(protocol.ImagePart{Mime: mimeType, VFSRef: vfsRef, AltText: name})
		return part, "image, uploaded", perr
	}
	part, perr := protocol.NewFilePart(protocol.FilePart{Mime: mimeType, VFSRef: vfsRef, FileName: name, SizeBytes: int64(len(data))})
	return part, "file, uploaded", perr
}

// artifactMime resolves a media type from the filename extension, falling back to
// content sniffing. The "; charset=..." suffix is stripped for a clean type.
func artifactMime(name string, data []byte) string {
	if m := mime.TypeByExtension(filepath.Ext(name)); m != "" {
		if i := strings.IndexByte(m, ';'); i >= 0 {
			m = m[:i]
		}
		return strings.TrimSpace(m)
	}
	return http.DetectContentType(data)
}

func artifactDataURI(mimeType string, data []byte) string {
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(data)
}
