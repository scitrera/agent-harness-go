package provider

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func imageMessage(t *testing.T, text string, img protocol.ImagePart) protocol.ChatMessage {
	t.Helper()
	parts := []protocol.ContentPart{}
	if text != "" {
		tp, err := protocol.NewTextPart(text)
		if err != nil {
			t.Fatalf("text part: %v", err)
		}
		parts = append(parts, tp)
	}
	ip, err := protocol.NewImagePart(img)
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	return protocol.ChatMessage{Role: protocol.RoleUser, Content: append(parts, ip)}
}

func TestOpenAILoweringEmitsImageBlocks(t *testing.T) {
	msg := imageMessage(t, "what is this?", protocol.ImagePart{DataURI: "data:image/png;base64,AAAA", Mime: "image/png"})
	body, err := json.Marshal(toOpenAIRequest(ChatRequest{Messages: []protocol.ChatMessage{msg}}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(body)
	if !strings.Contains(s, `"type":"image_url"`) || !strings.Contains(s, `data:image/png;base64,AAAA`) {
		t.Fatalf("expected image_url block with data uri, got %s", s)
	}
	if !strings.Contains(s, `"type":"text"`) || !strings.Contains(s, "what is this?") {
		t.Fatalf("expected text block alongside image, got %s", s)
	}
}

func TestOpenAILoweringPrefersURIDropsVFSRefOnly(t *testing.T) {
	// uri-only image is sent as the url.
	withURI := imageMessage(t, "", protocol.ImagePart{URI: "https://example.com/a.png"})
	body, _ := json.Marshal(toOpenAIRequest(ChatRequest{Messages: []protocol.ChatMessage{withURI}}))
	if !strings.Contains(string(body), "https://example.com/a.png") {
		t.Fatalf("uri image not lowered: %s", body)
	}

	// vfs_ref-only image cannot be resolved here -> dropped (no image_url block).
	vfsOnly := imageMessage(t, "hi", protocol.ImagePart{VFSRef: "vfs://blob/123"})
	body2, _ := json.Marshal(toOpenAIRequest(ChatRequest{Messages: []protocol.ChatMessage{vfsOnly}}))
	if strings.Contains(string(body2), "image_url") {
		t.Fatalf("vfs_ref-only image should be dropped on the openai path: %s", body2)
	}
	// Text-only content stays a plain string (no multimodal array).
	if !strings.Contains(string(body2), `"content":"hi"`) {
		t.Fatalf("expected plain string content when no resolvable image: %s", body2)
	}
}
