package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

type goalRecoveryHost struct {
	info      *sdk.TaskInfo
	found     bool
	lookupErr error
	completed int
	failed    int
}

func (h *goalRecoveryHost) Topic() string { return "agent-topic" }
func (h *goalRecoveryHost) LookupTaskInfo(context.Context, string) (*sdk.TaskInfo, bool, error) {
	return h.info, h.found, h.lookupErr
}
func (h *goalRecoveryHost) CompleteTask(context.Context, string) error     { h.completed++; return nil }
func (h *goalRecoveryHost) FailTask(context.Context, string, string) error { h.failed++; return nil }

type goalRecoveryRunner struct {
	resumeErr   error
	resumedAuth tools.MemoryAuthority
	resumed     int
	interrupted int
}

func (r *goalRecoveryRunner) ActiveTurnExecutions(context.Context) ([]turnjournal.Record, error) {
	return nil, nil
}
func (r *goalRecoveryRunner) ResumeTurn(ctx context.Context, _, _ string) (protocol.ChatMessage, error) {
	r.resumed++
	r.resumedAuth, _ = tools.MemoryAuthorityFrom(ctx)
	return protocol.ChatMessage{ID: "assistant-recovered"}, r.resumeErr
}
func (r *goalRecoveryRunner) InterruptTurn(context.Context, string, string, string) error {
	r.interrupted++
	return nil
}

func TestRecoverAetherTurnRestoresTypedAuthorityAndCompletes(t *testing.T) {
	host := &goalRecoveryHost{found: true, info: &sdk.TaskInfo{
		TaskID: "task-1", AssignedTo: "agent-topic", Status: pb.TaskStatus_TASK_STATUS_RUNNING.String(),
		Metadata:      map[string]string{"scitrera.logical_workspace": "project-a"},
		AuthorityMode: "on_behalf_of", AuthorityGrantID: "grant-1", SubjectType: "user", SubjectID: "alice",
	}}
	runner := &goalRecoveryRunner{}
	err := recoverAetherTurn(context.Background(), host, runner, turnjournal.Record{
		WorkspaceID: "project-a", SessionID: "session-1", TaskID: "task-1",
	})
	if err != nil || runner.resumed != 1 || host.completed != 1 || host.failed != 0 ||
		runner.resumedAuth != (tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"}) {
		t.Fatalf("err=%v runner=%#v host=%#v", err, runner, host)
	}
}

func TestRecoverAetherTurnFailsUnsafeResumeAndInterruptsAbsentTask(t *testing.T) {
	t.Run("unsafe resume", func(t *testing.T) {
		host := &goalRecoveryHost{found: true, info: &sdk.TaskInfo{
			TaskID: "task-1", AssignedTo: "agent-topic", Status: pb.TaskStatus_TASK_STATUS_RUNNING.String(),
		}}
		runner := &goalRecoveryRunner{resumeErr: errors.New("unsafe phase")}
		err := recoverAetherTurn(context.Background(), host, runner, turnjournal.Record{WorkspaceID: "project-a", TaskID: "task-1"})
		if err == nil || !strings.Contains(err.Error(), "unsafe phase") || host.failed != 1 || host.completed != 0 {
			t.Fatalf("err=%v runner=%#v host=%#v", err, runner, host)
		}
	})

	t.Run("absent task", func(t *testing.T) {
		host := &goalRecoveryHost{}
		runner := &goalRecoveryRunner{}
		err := recoverAetherTurn(context.Background(), host, runner, turnjournal.Record{WorkspaceID: "project-a", TaskID: "task-local"})
		if err == nil || runner.interrupted != 1 || runner.resumed != 0 {
			t.Fatalf("err=%v runner=%#v", err, runner)
		}
	})
}
