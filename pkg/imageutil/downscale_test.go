package imageutil

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func makePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), 128, 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func dims(t *testing.T, data []byte) (int, int) {
	t.Helper()
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	return cfg.Width, cfg.Height
}

func TestDownscale_LargerThanMaxEdge(t *testing.T) {
	out, mime, resized := Downscale(makePNG(t, 2400, 1200), 1200)
	if !resized {
		t.Fatal("expected resize")
	}
	if mime != "image/png" {
		t.Fatalf("mime = %q, want image/png", mime)
	}
	if w, h := dims(t, out); w != 1200 || h != 600 {
		t.Fatalf("dims = %dx%d, want 1200x600 (long edge capped, aspect preserved)", w, h)
	}
}

func TestDownscale_WithinMaxEdge_Untouched(t *testing.T) {
	src := makePNG(t, 800, 600)
	out, mime, resized := Downscale(src, 1200)
	if resized || mime != "" || !bytes.Equal(out, src) {
		t.Fatalf("already-small image must pass through untouched (resized=%v mime=%q)", resized, mime)
	}
}

func TestDownscale_OffAndNonImage(t *testing.T) {
	if _, _, r := Downscale(makePNG(t, 2400, 1200), 0); r {
		t.Fatal("maxEdge<=0 must be a no-op")
	}
	if out, _, r := Downscale([]byte("not an image"), 1200); r || string(out) != "not an image" {
		t.Fatal("non-image bytes must pass through untouched")
	}
}
