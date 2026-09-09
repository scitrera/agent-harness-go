// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func Test_Workspace_InspectFile_returns_multimodal_reference_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	ws := newTestWorkspace(t)
	img := image.NewRGBA(image.Rect(0, 0, 2, 3))
	img.Set(1, 2, color.RGBA{R: 255, A: 255})
	path := filepath.Join(ws.root, "images", "sample.png")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir image dir: %v", err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create image: %v", err)
	}
	if err := png.Encode(file, img); err != nil {
		t.Fatalf("encode image: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close image: %v", err)
	}

	// When
	info, err := ws.InspectFile(ctx, "images/sample.png")

	// Then
	if err != nil {
		t.Fatalf("inspect file: %v", err)
	}
	if info.URI != "workspace://images/sample.png" || info.MediaType != "image/png" || info.Kind != "image" {
		t.Fatalf("unexpected file identity: %#v", info)
	}
	if info.Width != 2 || info.Height != 3 || info.SHA256 == "" || info.SizeBytes == 0 {
		t.Fatalf("unexpected image metadata: %#v", info)
	}
}
