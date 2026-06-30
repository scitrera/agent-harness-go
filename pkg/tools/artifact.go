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

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const presentArtifactToolName = "present_artifact"

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
	err := reg.Register(presentArtifactToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		var args struct {
			Paths []string `json:"paths"`
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
		for _, p := range args.Paths {
			data, err := cfg.Workspace.ReadBytes(ctx, p, int64(maxBytes))
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
		}
		payload, err := json.Marshal(map[string]any{"presented": presented})
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
		Description: "Surface a file you saved in the workspace (e.g. a chart, image, or document) to the user as an inline artifact in your reply. Provide workspace path(s); save the file first, then present it. Images render inline in the chat; other files attach as downloads. Use this instead of a markdown image link — a local file path will NOT render.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"},"description":"Workspace file path(s) to present, e.g. [\"work/plot.png\"]"}},"required":["paths"]}`),
	})
	return nil
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
