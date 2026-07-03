package turn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// injectingRegistry returns a registry whose "inject" tool injects a QC image
// into the model's own context via Result.InjectUserParts.
func injectingRegistry(t *testing.T) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	if err := reg.Register("inject", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		res, err := tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"injected":"qc.png"}`))
		if err != nil {
			return tools.Result{}, err
		}
		img, err := protocol.NewImagePart(protocol.ImagePart{Mime: "image/png", DataURI: "data:image/png;base64,AAAA", AltText: "qc.png"})
		if err != nil {
			return tools.Result{}, err
		}
		label, err := protocol.NewTextPart("Rendered image for QC: qc.png")
		if err != nil {
			return tools.Result{}, err
		}
		res.InjectUserParts = []protocol.ContentPart{label, img}
		return res, nil
	})); err != nil {
		t.Fatalf("register inject tool: %v", err)
	}
	return reg
}

func injectProviderScript(t *testing.T) *scriptedProvider {
	t.Helper()
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "inject"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("looks good")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	return &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
}

// A tool that returns InjectUserParts with an image causes (a) a user-role
// message carrying the image to be appended AFTER the tool result, and (b) with a
// two-model registry starting on a non-vision model, the required capability
// flips to Vision and the next provider call runs on the vision-capable model.
func Test_Runner_Run_inject_image_appends_user_message_and_escalates(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	provider := injectProviderScript(t)
	reg := modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "light", Capabilities: modelpkg.Capabilities{Tools: true}},
		{Name: "vision", Capabilities: modelpkg.Capabilities{Tools: true, Vision: true}},
	}, "light")
	runner, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          injectingRegistry(t),
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 20}),
		Model:             "light",
		ModelRegistry:     reg,
		MaxToolIterations: 3,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	userPart, err := protocol.NewTextPart("make a chart")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}}); err != nil {
		t.Fatalf("run turn: %v", err)
	}

	// (a) history order: user, assistant(tool_call), tool_result, injected user
	// (image), final assistant. Find the injected user message after the tool
	// result and assert it carries an image.
	var injectedIdx = -1
	var toolResultIdx = -1
	for i, m := range store.messages {
		if m.Role == protocol.RoleToolResult {
			toolResultIdx = i
		}
		if i > 0 && m.Role == protocol.RoleUser && containsImage(m.Content) {
			injectedIdx = i
		}
	}
	if toolResultIdx < 0 {
		t.Fatalf("expected a tool-result message, got %#v", store.messages)
	}
	if injectedIdx < 0 {
		t.Fatalf("expected an injected user message carrying an image, got %#v", store.messages)
	}
	if injectedIdx <= toolResultIdx {
		t.Fatalf("injected user message (idx %d) must come after the tool result (idx %d)", injectedIdx, toolResultIdx)
	}

	// (b) escalation: the first provider call ran on the default non-vision model;
	// the second — after the image entered context — ran on the vision model.
	if len(provider.requests) != 2 {
		t.Fatalf("expected 2 provider requests, got %d", len(provider.requests))
	}
	if provider.requests[0].Model != "light" {
		t.Fatalf("first request model = %q, want light", provider.requests[0].Model)
	}
	if provider.requests[1].Model != "vision" {
		t.Fatalf("second request model = %q, want vision (escalated after image entered context)", provider.requests[1].Model)
	}
}

// With NO model registry (single-model mode) inject_image still injects the
// image, but there is nothing to escalate to, so the turn stays on the one model.
func Test_Runner_Run_inject_image_no_registry_stays_on_single_model(t *testing.T) {
	ctx := context.Background()
	provider := injectProviderScript(t)
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          injectingRegistry(t),
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 20}),
		Model:             "solo",
		MaxToolIterations: 3,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser}); err != nil {
		t.Fatalf("run turn: %v", err)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("expected 2 provider requests, got %d", len(provider.requests))
	}
	if provider.requests[1].Model != "solo" {
		t.Fatalf("single-model mode must stay on solo, got %q", provider.requests[1].Model)
	}
}

// escalateModel unit behavior: escalates only when the current model can't meet
// the requirement and a capable model exists; otherwise returns current.
func Test_escalateModel(t *testing.T) {
	reg := modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "light", Capabilities: modelpkg.Capabilities{Tools: true}},
		{Name: "vision", Capabilities: modelpkg.Capabilities{Tools: true, Vision: true}},
	}, "light")
	r := &Runner{model: "light", modelRegistry: reg, modelSelector: modelpkg.CapabilityDefault{}}
	ctx := context.Background()
	addr := protocol.MessageAddress{}
	user := protocol.ChatMessage{}
	visionReq := modelpkg.Capabilities{Tools: true, Vision: true}

	if got := r.escalateModel(ctx, addr, user, visionReq, "light"); got != "vision" {
		t.Fatalf("escalate light→vision, got %q", got)
	}
	// Current model already vision-capable → unchanged.
	if got := r.escalateModel(ctx, addr, user, visionReq, "vision"); got != "vision" {
		t.Fatalf("already-capable model should stay, got %q", got)
	}
	// No registry → single-model mode → unchanged.
	bare := &Runner{model: "solo"}
	if got := bare.escalateModel(ctx, addr, user, visionReq, "solo"); got != "solo" {
		t.Fatalf("no-registry escalate should stay solo, got %q", got)
	}
}
