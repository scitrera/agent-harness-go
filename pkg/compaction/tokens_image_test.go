package compaction

import (
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// A large inline data_uri image is charged the flat fixedImageTokens cost, NOT
// its base64 byte length / bytesPerToken — which would be far larger.
func TestEstimateTokens_imageChargedFixedCost(t *testing.T) {
	// ~40 KiB of base64 payload: as text this would be ~10k tokens.
	bigDataURI := "data:image/png;base64," + strings.Repeat("A", 40<<10)
	img, err := protocol.NewImagePart(protocol.ImagePart{Mime: "image/png", DataURI: bigDataURI})
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	msg := protocol.ChatMessage{Role: protocol.RoleUser, Content: []protocol.ContentPart{img}}

	got := EstimateTokens([]protocol.ChatMessage{msg})
	want := perMessageOverheadTokens + fixedImageTokens
	if got != want {
		t.Fatalf("image message estimate = %d, want %d (overhead + fixed image cost)", got, want)
	}

	// Sanity: the flat cost is far below charging the raw data_uri bytes as text.
	rawTextTokens := len(img.Raw()) / bytesPerToken
	if got >= rawTextTokens {
		t.Fatalf("image estimate %d should be well below the raw-bytes estimate %d", got, rawTextTokens)
	}
}

// Text parts are still charged by byte length; an image part added to a message
// only adds the flat image cost on top of the text estimate.
func TestEstimateTokens_textPlusImage(t *testing.T) {
	text, err := protocol.NewTextPart(strings.Repeat("x", 400)) // 400 bytes → 100 tokens
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	img, err := protocol.NewImagePart(protocol.ImagePart{Mime: "image/png", DataURI: "data:image/png;base64,AAAA"})
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	msg := protocol.ChatMessage{Role: protocol.RoleUser, Content: []protocol.ContentPart{text, img}}

	got := EstimateTokens([]protocol.ChatMessage{msg})
	want := perMessageOverheadTokens + len(text.Raw())/bytesPerToken + fixedImageTokens
	if got != want {
		t.Fatalf("text+image estimate = %d, want %d", got, want)
	}
}
