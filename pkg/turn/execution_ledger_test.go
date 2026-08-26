package turn

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/executionledger"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func newLedgerCommandRunner(t *testing.T, ledger *executionledger.Service) *Runner {
	t.Helper()
	runner, err := NewRunner(Config{
		Store:              &fakeStore{},
		Loader:             fakeLoader{},
		Provider:           &fakeProvider{},
		Assembler:          contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:              "model-a",
		ModelRegistry:      modelpkg.NewRegistry([]modelpkg.Model{{Name: "model-a"}, {Name: "model-b"}}, "model-a"),
		Commands:           commands.New(nil),
		DefaultWorkspaceID: "workspace-a",
		ExecutionLedger:    ledger,
		TurnObservers:      []hooks.TurnObserver{ledger},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestExecutionLedgerPersistsModelPinAcrossRunnerRestart(t *testing.T) {
	ctx := context.Background()
	store := executionledger.NewMemoryStore(executionledger.MemoryStoreConfig{})
	ledger := &executionledger.Service{Store: store, Authority: "test"}
	addr := protocol.MessageAddress{WorkspaceID: "workspace-a", ThreadID: "session-a", TaskID: "task-pin"}
	first := newLedgerCommandRunner(t, ledger)
	reply, err := first.Run(ctx, addr, userMessage(t, "/model model-b"))
	if err != nil || !strings.Contains(assistantPlainText(reply), "model-b") {
		t.Fatalf("pin reply=%q err=%v", assistantPlainText(reply), err)
	}
	if got := modelpkg.ActiveModelFromMessage(reply); got != "model-b" {
		t.Fatalf("pin reply active model=%q, want model-b", got)
	}

	second := newLedgerCommandRunner(t, ledger)
	addr.TaskID = "task-list"
	reply, err = second.Run(ctx, addr, userMessage(t, "/model"))
	if err != nil || !strings.Contains(assistantPlainText(reply), "Active: model-b") {
		t.Fatalf("restarted model reply=%q err=%v", assistantPlainText(reply), err)
	}
	if got := modelpkg.ActiveModelFromMessage(reply); got != "model-b" {
		t.Fatalf("restarted reply active model=%q, want model-b", got)
	}
	page, err := store.Query(ctx, executionledger.Ref{WorkspaceID: "workspace-a", SessionID: "session-a"}, executionledger.Query{Types: []executionledger.EventType{executionledger.EventModelPinned}})
	if err != nil || len(page.Events) != 1 || page.Events[0].Model != "model-b" {
		t.Fatalf("model pin events=%+v err=%v", page.Events, err)
	}
}

func TestExecutionLedgerBuiltinIsModelFree(t *testing.T) {
	store := executionledger.NewMemoryStore(executionledger.MemoryStoreConfig{})
	ledger := &executionledger.Service{Store: store, Authority: "local files"}
	runner := newLedgerCommandRunner(t, ledger)
	addr := protocol.MessageAddress{WorkspaceID: "workspace-a", ThreadID: "session-a", TaskID: "task-ledger"}
	reply, err := runner.Run(context.Background(), addr, userMessage(t, "/ledger --limit 5"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(assistantPlainText(reply), "Execution ledger") || !strings.Contains(runner.helpText(), "/ledger") {
		t.Fatalf("ledger reply/help missing: %q / %q", assistantPlainText(reply), runner.helpText())
	}
}
