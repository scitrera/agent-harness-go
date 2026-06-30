package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type fakeUploader struct {
	calls    int
	lastName string
	lastMime string
	lastSize int
}

func (u *fakeUploader) Upload(_ context.Context, _ protocol.MessageAddress, name, mimeType string, data []byte) (string, error) {
	u.calls++
	u.lastName = name
	u.lastMime = mimeType
	u.lastSize = len(data)
	return "vfs_test123", nil
}

func artifactWorkspace(t *testing.T, files map[string]string) *localtools.Workspace {
	t.Helper()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	for name, content := range files {
		if err := ws.WriteFile(context.Background(), name, content); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return ws
}

func invokeArtifact(t *testing.T, cfg ArtifactConfig, em *capturingEmitter, paths string) Result {
	t.Helper()
	reg := NewRegistry()
	if err := RegisterArtifact(reg, cfg); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx := WithPartEmitter(context.Background(), em)
	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: presentArtifactToolName, Arguments: json.RawMessage(`{"paths":` + paths + `}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	return res
}

func TestPresentArtifact_inlinesSmallImage(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"plot.png": "PNGBYTES"}) // 8 bytes
	up := &fakeUploader{}
	em := &capturingEmitter{}
	res := invokeArtifact(t, ArtifactConfig{Workspace: ws, Uploader: up, InlineMaxBytes: 16}, em, `["plot.png"]`)
	if res.IsError {
		t.Fatalf("unexpected error result: %s", res.Payload)
	}
	if up.calls != 0 {
		t.Fatalf("small image must not upload, got %d calls", up.calls)
	}
	if len(em.parts) != 1 {
		t.Fatalf("expected 1 emitted part, got %d", len(em.parts))
	}
	img, ok := em.parts[0].AsImage()
	if !ok {
		t.Fatalf("expected image part, got %s", em.parts[0].Type())
	}
	if img.VFSRef != "" || !strings.HasPrefix(img.DataURI, "data:image/png;base64,") {
		t.Fatalf("expected inline data_uri, no vfs_ref: %+v", img)
	}
}

func TestPresentArtifact_uploadsLargeImage(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"big.png": strings.Repeat("x", 64)})
	up := &fakeUploader{}
	em := &capturingEmitter{}
	invokeArtifact(t, ArtifactConfig{Workspace: ws, Uploader: up, InlineMaxBytes: 16}, em, `["big.png"]`)
	if up.calls != 1 || up.lastMime != "image/png" {
		t.Fatalf("expected one image upload, got calls=%d mime=%q", up.calls, up.lastMime)
	}
	img, ok := em.parts[0].AsImage()
	if !ok || img.VFSRef != "vfs_test123" || img.DataURI != "" {
		t.Fatalf("expected uploaded image part with vfs_ref, no data_uri: %+v ok=%v", img, ok)
	}
}

func TestPresentArtifact_uploadsNonImageAsFile(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"report.pdf": "%PDF-1.4 data"})
	up := &fakeUploader{}
	em := &capturingEmitter{}
	invokeArtifact(t, ArtifactConfig{Workspace: ws, Uploader: up, InlineMaxBytes: 16}, em, `["report.pdf"]`)
	if up.calls != 1 {
		t.Fatalf("expected one file upload, got %d", up.calls)
	}
	f, ok := em.parts[0].AsFile()
	if !ok || f.VFSRef != "vfs_test123" || f.FileName != "report.pdf" {
		t.Fatalf("expected file part with vfs_ref + filename: %+v ok=%v", f, ok)
	}
}

func TestPresentArtifact_nonImageWithoutUploaderErrors(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"report.pdf": "%PDF-1.4 data"})
	em := &capturingEmitter{}
	res := invokeArtifact(t, ArtifactConfig{Workspace: ws, Uploader: nil, InlineMaxBytes: 16}, em, `["report.pdf"]`)
	if !res.IsError {
		t.Fatal("expected an error result presenting a non-image file with no uploader")
	}
	if len(em.parts) != 0 {
		t.Fatalf("nothing should be emitted on error, got %d", len(em.parts))
	}
}

func TestPresentArtifact_requiresPaths(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{})
	em := &capturingEmitter{}
	res := invokeArtifact(t, ArtifactConfig{Workspace: ws}, em, `[]`)
	if !res.IsError {
		t.Fatal("expected an error result when no paths are given")
	}
}
