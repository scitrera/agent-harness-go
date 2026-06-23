package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

type spySubagent struct {
	called bool
	task   string
	depth  int
}

func (s *spySubagent) RunSubagent(_ context.Context, req subagent.Request) (subagent.Result, error) {
	s.called = true
	s.task = req.Task
	s.depth = req.Depth
	return subagent.Result{Text: "sub-agent answer"}, nil
}

func TestRegisterSubagentInvokes(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Descriptor is surfaced to the model.
	found := false
	for _, d := range reg.Descriptors() {
		if d.Name == "spawn_subagent" {
			found = true
		}
	}
	if !found {
		t.Fatal("spawn_subagent descriptor not registered")
	}

	res, err := reg.Invoke(context.Background(), Request{CallID: "c1", Name: "spawn_subagent", Arguments: json.RawMessage(`{"task":"research X"}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !spy.called || spy.task != "research X" || spy.depth != 1 {
		t.Fatalf("runner not invoked correctly: %+v", spy)
	}
	if !bytes.Contains(res.Payload, []byte("sub-agent answer")) {
		t.Fatalf("result missing sub-agent answer: %s", res.Payload)
	}
}

func TestRegisterSubagentDepthLimit(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Already at the depth limit -> denied without invoking the runner.
	ctx := subagent.WithDepth(context.Background(), 2)
	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "spawn_subagent", Arguments: json.RawMessage(`{"task":"nested"}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if spy.called {
		t.Fatal("runner should not be invoked at the depth limit")
	}
	if !res.IsError || !bytes.Contains(res.Payload, []byte("depth limit")) {
		t.Fatalf("expected depth-limit error result, got %s (isError=%v)", res.Payload, res.IsError)
	}
}
