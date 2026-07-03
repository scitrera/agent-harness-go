package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func invokeInjectImage(t *testing.T, cfg InjectImageConfig, args string) Result {
	t.Helper()
	reg := NewRegistry()
	if err := RegisterInjectImage(reg, cfg); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      injectImageToolName,
		Arguments: json.RawMessage(args),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	return res
}

func TestInjectImage_injectsImageAsUserPart(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"plot.png": "PNGBYTES"})
	res := invokeInjectImage(t, InjectImageConfig{Workspace: ws}, `{"path":"plot.png"}`)
	if res.IsError {
		t.Fatalf("unexpected error result: %s", res.Payload)
	}
	// Ack payload names the injected file + byte count.
	var ack struct {
		Injected string `json:"injected"`
		Bytes    int    `json:"bytes"`
	}
	if err := json.Unmarshal(res.Payload, &ack); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if ack.Injected != "plot.png" || ack.Bytes != len("PNGBYTES") {
		t.Fatalf("ack = %+v, want plot.png / 8 bytes", ack)
	}
	// The injected user parts are a text label followed by the image.
	if len(res.InjectUserParts) != 2 {
		t.Fatalf("expected [label, image] inject parts, got %d", len(res.InjectUserParts))
	}
	if res.InjectUserParts[0].Type() != protocol.ContentText {
		t.Fatalf("first inject part should be the text label, got %s", res.InjectUserParts[0].Type())
	}
	img, ok := res.InjectUserParts[1].AsImage()
	if !ok {
		t.Fatalf("second inject part should be an image, got %s", res.InjectUserParts[1].Type())
	}
	if !strings.HasPrefix(img.DataURI, "data:image/png;base64,") {
		t.Fatalf("expected inline base64 data_uri, got %q", img.DataURI)
	}
	if img.AltText != "plot.png" {
		t.Fatalf("expected default alt = filename, got %q", img.AltText)
	}
	// Nothing co-locates on the tool result (images ride the user message, not the
	// tool-role result which is text-only).
	if len(res.Parts) != 0 {
		t.Fatalf("expected no tool-result Parts, got %d", len(res.Parts))
	}
}

func TestInjectImage_nonImageErrors(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"notes.txt": "just text"})
	res := invokeInjectImage(t, InjectImageConfig{Workspace: ws}, `{"path":"notes.txt"}`)
	if !res.IsError {
		t.Fatal("expected an error result injecting a non-image file")
	}
	if len(res.InjectUserParts) != 0 {
		t.Fatalf("nothing should be injected on error, got %d parts", len(res.InjectUserParts))
	}
}

func TestInjectImage_overMaxBytesErrors(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{"big.png": strings.Repeat("x", 64)})
	res := invokeInjectImage(t, InjectImageConfig{Workspace: ws, MaxBytes: 16}, `{"path":"big.png"}`)
	if !res.IsError {
		t.Fatal("expected an error result for an over-MaxBytes image")
	}
	if !strings.Contains(string(res.Payload), "downscale") {
		t.Fatalf("expected a downscale hint in the error, got %s", res.Payload)
	}
	if len(res.InjectUserParts) != 0 {
		t.Fatalf("nothing should be injected on error, got %d parts", len(res.InjectUserParts))
	}
}

func TestInjectImage_requiresPath(t *testing.T) {
	ws := artifactWorkspace(t, map[string]string{})
	res := invokeInjectImage(t, InjectImageConfig{Workspace: ws}, `{}`)
	if !res.IsError {
		t.Fatal("expected an error result when no path is given")
	}
}
