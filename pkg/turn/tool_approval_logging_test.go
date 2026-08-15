package turn

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// An approval request that cannot reach the user is the one failure mode that
// GUARANTEES a hung turn: the runner goes on to block in approvals.Await for a
// decision nobody was asked to make, and the user sees a chat that produces
// nothing at all. Both ways of losing it used to be silent — a nil emitter
// returned early, and a failed pending emit logged at WARN alongside failures
// that only lose a record of an already-made decision. These pin that the
// turn-killing cases are loud and name the tool.

// captureLogs swaps the default slog logger for the duration of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

type failingEmitter struct{ err error }

func (f failingEmitter) UpsertPart(context.Context, protocol.ContentPart) error { return f.err }

func pendingCall() protocol.ToolInvokeEnvelope {
	return protocol.ToolInvokeEnvelope{Name: "search_tools"}
}

func Test_emitApproval_without_emitter_logs_error(t *testing.T) {
	buf := captureLogs(t)
	r := &Runner{}

	r.emitApproval(context.Background(), nil, "call-1", pendingCall(),
		[]string{"once"}, spec.ApprovalPending, "tool is not pre-authorized")

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Fatalf("a prompt the user can never see must be an error; got:\n%s", out)
	}
	if !strings.Contains(out, "search_tools") {
		t.Fatalf("the log must name the tool that will hang; got:\n%s", out)
	}
}

func Test_emitApproval_pending_emit_failure_logs_error(t *testing.T) {
	buf := captureLogs(t)
	r := &Runner{}
	emitter := failingEmitter{err: errors.New("aether publish denied")}

	r.emitApproval(context.Background(), emitter, "call-2", pendingCall(),
		[]string{"once"}, spec.ApprovalPending, "tool is not pre-authorized")

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Fatalf("a failed PENDING emit hangs the turn; got:\n%s", out)
	}
	if !strings.Contains(out, "aether publish denied") {
		t.Fatalf("the underlying cause must survive; got:\n%s", out)
	}
}

func Test_emitApproval_resolved_emit_failure_stays_a_warning(t *testing.T) {
	// The decision was already made and acted on; only the record of it is
	// lost, so this must NOT be escalated alongside the hang-causing cases.
	buf := captureLogs(t)
	r := &Runner{}
	emitter := failingEmitter{err: errors.New("aether publish denied")}

	r.emitApproval(context.Background(), emitter, "call-3", pendingCall(),
		[]string{"once"}, spec.ApprovalApproved, "")

	out := buf.String()
	if strings.Contains(out, "level=ERROR") {
		t.Fatalf("losing a resolved-status part is not turn-fatal; got:\n%s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("it should still be reported; got:\n%s", out)
	}
}

// Guards the signal the nil-emitter error depends on: the authorizer must not
// discard the ok from PartEmitterFrom, or the condition that guarantees a hung
// turn is invisible at the point it is decided.
func Test_PartEmitterFrom_reports_absence(t *testing.T) {
	if _, ok := tools.PartEmitterFrom(context.Background()); ok {
		t.Fatal("a bare context must report no emitter")
	}
}
