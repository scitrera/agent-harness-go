package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/imageutil"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const injectImageToolName = "inject_image"

// maxInjectImages caps how many images one inject_image call may carry. Batching
// pages into a single call (like present_artifact's paths) collapses N tool
// iterations into one — essential when reading many rendered PDF pages via vision.
const maxInjectImages = 10

// defaultInjectImageMaxBytes caps a single injected image. Kept well under
// present_artifact's read cap: an image the model injects into its own context
// rides inline as a base64 data_uri on every subsequent provider call, so a large
// one bloats the request. 5 MiB comfortably covers a rendered chart/slide/PDF
// page while keeping the per-call payload sane.
const defaultInjectImageMaxBytes = 5 << 20 // 5 MiB

// InjectImageConfig configures the inject_image tool. It mirrors the artifact
// tool's path-resolution seam (Workspace + SessionDir) so a bare relative path
// resolves to the code session's working dir (where the python tool saves).
type InjectImageConfig struct {
	// Workspace reads the image bytes the agent names. Required.
	Workspace *localtools.Workspace
	// SessionDir, when set, returns the per-(thread,context) working directory a
	// code session writes relative files into (sahara's /sahara/sessions/<key>).
	// A RELATIVE image path resolves against it first, falling back to the
	// workspace root. nil → workspace-root only. Shares the artifact tool's
	// derivation; the distribution owns it.
	SessionDir func(threadID, context string) string
	// MaxBytes hard-caps a single injected image read (<=0 → 5 MiB).
	MaxBytes int
	// MaxImageEdge, when > 0, downscales an injected image so its longest edge is
	// <= MaxImageEdge before it's inlined as a data_uri — bounding the vision-token
	// cost of a high-resolution screenshot / PDF-page render. Only the model-bound
	// copy is scaled; the on-disk artifact is untouched. <=0 → no downscale.
	MaxImageEdge int
}

// RegisterInjectImage registers inject_image: the agent names a local image it
// produced (a chart it saved, a rendered slide/PDF page) and the harness injects
// it as a user-role image message into the agent's OWN context so it can visually
// inspect its own work before finishing. The image snapshot rides as a base64
// data_uri (reusing the provider passthrough — no provider change); when the
// image enters context the harness escalates the rest of the turn to a
// vision-capable model.
func RegisterInjectImage(reg *Registry, cfg InjectImageConfig) error {
	if cfg.Workspace == nil {
		return fmt.Errorf("%w: inject_image workspace required", ErrInvalidTool)
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultInjectImageMaxBytes
	}
	// readArtifactBytes takes an ArtifactConfig; build one from the shared seams so
	// path resolution (session-dir → workspace-root) matches present_artifact.
	readCfg := ArtifactConfig{Workspace: cfg.Workspace, SessionDir: cfg.SessionDir}
	err := reg.Register(injectImageToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		var args struct {
			Path    string   `json:"path"`
			Paths   []string `json:"paths"`
			Context string   `json:"context"`
			Alt     string   `json:"alt"`
		}
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
		// Accept a single `path` (back-compat) or a `paths` array; batching many
		// rendered pages into one call keeps the tool-iteration count sane.
		paths := args.Paths
		if len(paths) == 0 && args.Path != "" {
			paths = []string{args.Path}
		}
		if len(paths) == 0 {
			return errorResult(req, "path/paths is required: name one or more local image files to inject")
		}
		if len(paths) > maxInjectImages {
			return errorResult(req, fmt.Sprintf("inject_image accepts at most %d images per call; got %d — split into multiple calls", maxInjectImages, len(paths)))
		}
		imgs := make([]protocol.ContentPart, 0, len(paths))
		names := make([]string, 0, len(paths))
		total := 0
		for _, p := range paths {
			data, err := readArtifactBytes(ctx, readCfg, req.Addr.ThreadID, args.Context, p, int64(maxBytes))
			if err != nil {
				// A size-cap breach is reported as an actionable "downscale it" hint (no
				// auto-downscale — that's a deliberate follow-on); any other read failure
				// (missing file, etc.) reports the raw error so the model can adapt.
				if errors.Is(err, localtools.ErrInvalidFile) {
					return errorResult(req, fmt.Sprintf("%s exceeds the %d-byte inject_image limit; re-render or downscale it smaller (e.g. fewer pixels or lower DPI) and try again", filepath.Base(p), maxBytes))
				}
				return errorResult(req, fmt.Sprintf("read %s: %v", p, err))
			}
			name := filepath.Base(p)
			mimeType := artifactMime(name, data)
			if !strings.HasPrefix(mimeType, "image/") {
				return errorResult(req, fmt.Sprintf("inject_image only accepts images; %s is %s", name, mimeType))
			}
			// Cap the injected resolution to bound vision-token cost (the on-disk
			// artifact is left untouched — only this model-bound copy is downscaled).
			if cfg.MaxImageEdge > 0 {
				if scaled, m, ok := imageutil.Downscale(data, cfg.MaxImageEdge); ok {
					data = scaled
					mimeType = m
				}
			}
			img, err := protocol.NewImagePart(protocol.ImagePart{Mime: mimeType, DataURI: artifactDataURI(mimeType, data), AltText: name})
			if err != nil {
				return errorResult(req, fmt.Sprintf("build image part for %s: %v", name, err))
			}
			imgs = append(imgs, img)
			names = append(names, name)
			total += len(data)
		}
		// A short text label makes the injected user message self-describing (an
		// image-only user message reads as an unexplained attachment).
		labelText := "Rendered image(s) for QC: " + strings.Join(names, ", ")
		if args.Alt != "" {
			labelText = args.Alt + " — " + strings.Join(names, ", ")
		}
		label, err := protocol.NewTextPart(labelText)
		if err != nil {
			return Result{}, err
		}
		payload, err := json.Marshal(map[string]any{"injected": names, "count": len(names), "bytes": total})
		if err != nil {
			return Result{}, err
		}
		res, err := NewJSONResult(req.CallID, req.Name, payload)
		if err != nil {
			return Result{}, err
		}
		res.InjectUserParts = append([]protocol.ContentPart{label}, imgs...)
		return res, nil
	}))
	if err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        injectImageToolName,
		Description: "Inject one or more local images you produced (a chart you saved, or rendered slide/PDF pages) into your OWN context so you can visually read/inspect them. Pass MULTIPLE images in a single call via `paths` (up to 10) — e.g. render several PDF pages and read them all in one call instead of one-per-call, which keeps your tool-iteration budget for the rest of the work. A bare relative path resolves to your code session dir (where the python tool saves). Requires a vision-capable model; the harness routes the rest of this turn to one automatically. Use this to READ rendered pages or CHECK YOUR OWN WORK.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"paths":{"type":"array","items":{"type":"string"},"maxItems":10,"description":"Image file(s) to inject in ONE call (up to 10); a bare name resolves to your code session dir, e.g. [\"pg-01.png\",\"pg-02.png\"]"},"path":{"type":"string","description":"A single image file (alternative to paths for one image)"},"context":{"type":"string","description":"Optional: the code context label the files were saved under (omit for the main session)"},"alt":{"type":"string","description":"Optional: a short description of what the images should show"}}}`),
	})
	return nil
}
