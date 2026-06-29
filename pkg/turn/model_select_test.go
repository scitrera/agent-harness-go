package turn

import (
	"context"
	"testing"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestResolveTurnModel(t *testing.T) {
	reg := modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "light", Capabilities: modelpkg.Capabilities{Tools: true}},
		{Name: "vision", Capabilities: modelpkg.Capabilities{Vision: true, Tools: true}},
	}, "light")
	ctx := context.Background()
	addr := protocol.MessageAddress{}
	textMsg := protocol.ChatMessage{}
	imgPart, err := protocol.NewImagePart(protocol.ImagePart{DataURI: "data:image/png;base64,AA=="})
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	imgMsg := protocol.ChatMessage{Content: []protocol.ContentPart{imgPart}}

	// No registry/selector → always the single configured model (unchanged behavior).
	bare := &Runner{model: "light"}
	if got := bare.resolveTurnModel(ctx, addr, imgMsg, ""); got != "light" {
		t.Fatalf("no registry want light, got %q", got)
	}

	r := &Runner{model: "light", modelRegistry: reg, modelSelector: modelpkg.CapabilityDefault{}}

	// Explicit override wins even with a registry + an image turn.
	if got := r.resolveTurnModel(ctx, addr, imgMsg, "override-x"); got != "override-x" {
		t.Fatalf("override want override-x, got %q", got)
	}
	// Text turn → default suffices (capability-matched).
	if got := r.resolveTurnModel(ctx, addr, textMsg, ""); got != "light" {
		t.Fatalf("text turn want light, got %q", got)
	}
	// Image turn → capability fallback to the vision-capable model (default lacks Vision).
	if got := r.resolveTurnModel(ctx, addr, imgMsg, ""); got != "vision" {
		t.Fatalf("image turn want vision (capability routing), got %q", got)
	}
}
