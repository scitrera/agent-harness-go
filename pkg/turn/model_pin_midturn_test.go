// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

// modelRecordingProvider records the model of each Chat call and replays a
// scripted response sequence (the last response repeats).
type modelRecordingProvider struct {
	models    []string
	responses []provider.ChatResponse
	i         int
}

func (p *modelRecordingProvider) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.models = append(p.models, req.Model)
	resp := p.responses[p.i]
	if p.i < len(p.responses)-1 {
		p.i++
	}
	return resp, nil
}

// A tool that pins a model mid-turn (load_skill honoring a skill's preferred_model
// does this via the ModelPreference seam) must take effect on the NEXT provider
// call THIS turn — the turn's model is resolved once before any tool runs, and in
// a per-turn harness the in-memory pin would never survive to a later turn.
func Test_Runner_adopts_pinned_model_midturn(t *testing.T) {
	reg := modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "default-model", Capabilities: modelpkg.Capabilities{Tools: true}},
		{Name: "pinned-model", Capabilities: modelpkg.Capabilities{Tools: true}},
	}, "default-model")

	registry := tools.NewRegistry()
	if err := registry.Register("pin", tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
		if pref, ok := tools.ModelPreferenceFrom(ctx); ok {
			pref("pinned-model")
		}
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}

	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "c1", Name: "pin", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	prov := &modelRecordingProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}

	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          prov,
		Publisher:         &fakePublisher{},
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		Model:             "default-model",
		ModelRegistry:     reg,
		MaxToolIterations: 3,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userTurn(t)); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(prov.models) < 2 {
		t.Fatalf("expected >= 2 provider calls, got models=%v", prov.models)
	}
	if prov.models[0] != "default-model" {
		t.Fatalf("first call model = %q, want default-model (pin not yet set)", prov.models[0])
	}
	if prov.models[1] != "pinned-model" {
		t.Fatalf("second call model = %q, want pinned-model (mid-turn pin must take effect)", prov.models[1])
	}
}
