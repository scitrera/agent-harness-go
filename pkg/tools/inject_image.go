package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const injectImageToolName = "inject_image"

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
	// code session writes relative files into (sahara's /workspace/sessions/<key>).
	// A RELATIVE image path resolves against it first, falling back to the
	// workspace root. nil → workspace-root only. Shares the artifact tool's
	// derivation; the distribution owns it.
	SessionDir func(threadID, context string) string
	// MaxBytes hard-caps a single injected image read (<=0 → 5 MiB).
	MaxBytes int
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
			Path    string `json:"path"`
			Context string `json:"context"`
			Alt     string `json:"alt"`
		}
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
		if args.Path == "" {
			return errorResult(req, "path is required: name the local image file to inject")
		}
		data, err := readArtifactBytes(ctx, readCfg, req.Addr.ThreadID, args.Context, args.Path, int64(maxBytes))
		if err != nil {
			// A size-cap breach is reported as an actionable "downscale it" hint (no
			// auto-downscale — that's a deliberate follow-on); any other read failure
			// (missing file, etc.) reports the raw error so the model can adapt.
			if errors.Is(err, localtools.ErrInvalidFile) {
				return errorResult(req, fmt.Sprintf("%s exceeds the %d-byte inject_image limit; re-render or downscale it smaller (e.g. fewer pixels or lower DPI) and try again", filepath.Base(args.Path), maxBytes))
			}
			return errorResult(req, fmt.Sprintf("read %s: %v", args.Path, err))
		}
		name := filepath.Base(args.Path)
		mimeType := artifactMime(name, data)
		if !strings.HasPrefix(mimeType, "image/") {
			return errorResult(req, fmt.Sprintf("inject_image only accepts images; %s is %s", name, mimeType))
		}
		alt := args.Alt
		if alt == "" {
			alt = name
		}
		img, err := protocol.NewImagePart(protocol.ImagePart{Mime: mimeType, DataURI: artifactDataURI(mimeType, data), AltText: alt})
		if err != nil {
			return errorResult(req, fmt.Sprintf("build image part for %s: %v", name, err))
		}
		// A short text label makes the injected user message self-describing (an
		// image-only user message reads as an unexplained attachment).
		label, err := protocol.NewTextPart("Rendered image for QC: " + name)
		if err != nil {
			return Result{}, err
		}
		payload, err := json.Marshal(map[string]any{"injected": name, "bytes": len(data)})
		if err != nil {
			return Result{}, err
		}
		res, err := NewJSONResult(req.CallID, req.Name, payload)
		if err != nil {
			return Result{}, err
		}
		res.InjectUserParts = []protocol.ContentPart{label, img}
		return res, nil
	}))
	if err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        injectImageToolName,
		Description: "Inject a local image you produced (a chart you saved, or a rendered slide/PDF page) into your OWN context so you can visually inspect it — QC for overflow, clipping, low contrast, wrong data, placeholder residue, or wrong order — then fix and re-render. A bare relative path resolves to your code session dir (where the python tool saves). Requires a vision-capable model; the harness routes the rest of this turn to one automatically. Use this to CHECK YOUR OWN WORK before finishing.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Image file to inject; a bare name resolves to your code session dir, e.g. \"slide1.png\""},"context":{"type":"string","description":"Optional: the code context label the file was saved under (omit for the main session)"},"alt":{"type":"string","description":"Optional: a short description of what the image should show"}},"required":["path"]}`),
	})
	return nil
}
