package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

func Test_Runner_RunSubagent_returns_final_text(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:   "summarize the design",
		Depth:  1,
		Parent: protocol.MessageAddress{ThreadID: "t1"},
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if res.Text != "assistant response" {
		t.Fatalf("expected sub-agent final text, got %q", res.Text)
	}
}
