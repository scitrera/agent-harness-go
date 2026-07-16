// Package imageutil holds shared image transforms for the model-bound image path.
// The one transform today is Downscale: cap an image's longest edge before it is
// inlined as a data_uri, so a full-resolution screenshot/PDF-page render doesn't
// cost full vision tokens on every provider call. It operates ONLY on the copy
// sent to the model — the on-disk materialized blob stays full resolution for the
// file tools.
package imageutil

import (
	"bytes"
	"image"
	// Decoders for the common inbound formats (screenshots, PDF-page renders,
	// photos). Registered for image.Decode's sniffing.
	_ "image/gif"
	_ "image/jpeg"
	"image/png"

	xdraw "golang.org/x/image/draw"
	_ "golang.org/x/image/webp" // decode webp inputs (read-only)
)

// Downscale returns image bytes whose longest edge is <= maxEdge, re-encoded as
// PNG (lossless — preserves text/screenshots without artifacts). It is a best-effort
// no-op that returns the ORIGINAL bytes with resized=false when:
//   - maxEdge <= 0 (feature off) or data is empty,
//   - the bytes aren't a decodable image,
//   - the image already fits within maxEdge, or
//   - decode/scale/encode fails.
//
// On a successful resize it returns the new bytes, mime "image/png", and true. The
// PNG byte size may exceed the source for a downscaled photo, but that doesn't
// matter for vision tokens (the model rasterizes by pixels, and the caller's
// byte-size caps still apply); reducing PIXELS is what cuts the token cost.
func Downscale(data []byte, maxEdge int) (out []byte, mime string, resized bool) {
	if maxEdge <= 0 || len(data) == 0 {
		return data, "", false
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return data, "", false // not a decodable image — leave untouched
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	longest := w
	if h > longest {
		longest = h
	}
	if longest <= maxEdge || longest == 0 {
		return data, "", false // already within the cap
	}
	scale := float64(maxEdge) / float64(longest)
	nw := int(float64(w) * scale)
	nh := int(float64(h) * scale)
	if nw < 1 {
		nw = 1
	}
	if nh < 1 {
		nh = 1
	}
	dst := image.NewRGBA(image.Rect(0, 0, nw, nh))
	// CatmullRom: high-quality resampling — keeps text/lines legible after the
	// downscale (nearest-neighbor would alias screenshots badly).
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), img, b, xdraw.Over, nil)
	var buf bytes.Buffer
	if err := png.Encode(&buf, dst); err != nil {
		return data, "", false
	}
	return buf.Bytes(), "image/png", true
}
