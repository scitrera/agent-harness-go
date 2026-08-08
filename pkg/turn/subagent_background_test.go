package turn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

// captureEnqueuer records inbound turns pushed by a background sub-agent's
// completion, signalling each on a buffered channel so tests can await the push.
type captureEnqueuer struct {
	done chan channel.Inbound
}

func newCaptureEnqueuer() *captureEnqueuer {
	return &captureEnqueuer{done: make(chan channel.Inbound, 8)}
}

func (c *captureEnqueuer) Enqueue(_ context.Context, in channel.Inbound) error {
	c.done <- in
	return nil
}

// bgErrProvider always fails, to exercise the failed-completion path.
type bgErrProvider struct{}

func (bgErrProvider) Chat(context.Context, provider.ChatRequest) (provider.ChatResponse, error) {
	return provider.ChatResponse{}, errors.New("provider boom")
}

func findSubagentPart(msg protocol.ChatMessage) (protocol.SubagentPart, bool) {
	for _, p := range msg.Content {
		if sp, ok := p.AsSubagent(); ok {
			return sp, true
		}
	}
	return protocol.SubagentPart{}, false
}

func handoffToken(meta map[string]json.RawMessage) string {
	raw, ok := meta["scitrera"]
	if !ok {
		return ""
	}
	var h struct {
		Token string `json:"authority_handoff"`
	}
	_ = json.Unmarshal(raw, &h)
	return h.Token
}

func Test_Runner_StartBackground_pushes_completion_notice_to_parent(t *testing.T) {
	enq := newCaptureEnqueuer()
	handoff := authhandoff.New()
	r, err := NewRunner(Config{
		Store:       &fakeStore{},
		Loader:      fakeLoader{},
		Provider:    &fakeProvider{},
		Assembler:   contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Notifier:    enq,
		AuthHandoff: handoff,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	threadID, err := r.StartBackground(context.Background(), subagent.Request{
		Task:        "do it in the background",
		Depth:       1,
		Parent:      protocol.MessageAddress{ThreadID: "parent-1"},
		SubjectType: "user",
		SubjectID:   "u9",
	})
	if err != nil {
		t.Fatalf("start background: %v", err)
	}
	// The handle is returned immediately, before the child finishes.
	if !strings.HasPrefix(threadID, "parent-1::sub::") {
		t.Fatalf("child thread id = %q, want parent-1::sub:: prefix", threadID)
	}

	select {
	case in := <-enq.done:
		// The notice wakes a fresh turn on the PARENT thread, with a distinct task id
		// so it never clobbers the parent's in-flight turn.
		if in.Addr.ThreadID != "parent-1" {
			t.Fatalf("notice thread = %q, want parent-1", in.Addr.ThreadID)
		}
		if in.Addr.TaskID == "" {
			t.Fatal("notice must carry a fresh TaskID")
		}
		sp, ok := findSubagentPart(in.Message)
		if !ok {
			t.Fatalf("notice missing SubagentPart: %+v", in.Message.Content)
		}
		if sp.Status != protocol.SubagentCompleted {
			t.Fatalf("notice status = %q, want completed", sp.Status)
		}
		if sp.ThreadID != threadID {
			t.Fatalf("notice thread handle = %q, want %q", sp.ThreadID, threadID)
		}
		// The OBO is handed off as an opaque single-use token, NOT a credential on
		// the message; resolving it yields the parent subject, and it is consumed.
		tok := handoffToken(in.Message.Meta)
		if tok == "" {
			t.Fatal("notice missing authority handoff token")
		}
		auth, ok := handoff.Resolve(tok)
		if !ok || auth.SubjectID != "u9" || auth.SubjectType != "user" {
			t.Fatalf("resolved auth = %+v (ok=%v), want subject u9/user", auth, ok)
		}
		if _, again := handoff.Resolve(tok); again {
			t.Fatal("handoff token must be single-use")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no completion notice was pushed")
	}
}

func Test_Runner_StartBackground_usesDurableTaskLifecycle(t *testing.T) {
	enq := newCaptureEnqueuer()
	tasks := &captureSubagentTasks{}
	observer := &captureSubagentLifecycle{}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Notifier:  enq, SubagentTasks: tasks, SubagentObserver: observer, SubagentDefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	threadID, err := runner.StartBackground(context.Background(), subagent.Request{
		Task: "background review", InvocationID: "tool-call-bg",
		Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-1", TaskID: "parent-task"},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-enq.done:
	case <-time.After(3 * time.Second):
		t.Fatal("no task-backed background completion notice")
	}
	calls, admission, _ := tasks.snapshot()
	if strings.Join(calls, ",") != "admit,start,finish:completed" || !admission.Background || admission.ChildSessionID != threadID {
		t.Fatalf("calls=%#v admission=%+v", calls, admission)
	}
	events := observer.snapshot()
	if len(events) != 3 || events[0].Record.TaskID != "aether-child-task" || events[2].Record.Status != spec.SessionSubagentCompleted {
		t.Fatalf("lifecycle events = %#v", events)
	}
}

func Test_Runner_StartBackground_survives_parent_cancel(t *testing.T) {
	enq := newCaptureEnqueuer()
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Notifier:  enq,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := r.StartBackground(ctx, subagent.Request{
		Task:   "detached work",
		Parent: protocol.MessageAddress{ThreadID: "parent-1"},
	}); err != nil {
		t.Fatalf("start background: %v", err)
	}
	// Cancelling the spawning turn's context must NOT abort the detached child.
	cancel()
	select {
	case in := <-enq.done:
		if in.Addr.ThreadID != "parent-1" {
			t.Fatalf("notice thread = %q, want parent-1", in.Addr.ThreadID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("detached child did not complete after parent cancel")
	}
}

func Test_Runner_StartBackground_reports_failure_status(t *testing.T) {
	enq := newCaptureEnqueuer()
	r, err := NewRunner(Config{
		Store:               &fakeStore{},
		Loader:              fakeLoader{},
		Provider:            bgErrProvider{},
		Assembler:           contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Notifier:            enq,
		MaxTransientRetries: -1, // surface the provider error immediately (no retries)
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.StartBackground(context.Background(), subagent.Request{
		Task:   "will fail",
		Parent: protocol.MessageAddress{ThreadID: "parent-1"},
	}); err != nil {
		t.Fatalf("start background: %v", err)
	}
	select {
	case in := <-enq.done:
		sp, ok := findSubagentPart(in.Message)
		if !ok {
			t.Fatalf("notice missing SubagentPart: %+v", in.Message.Content)
		}
		if sp.Status != protocol.SubagentFailed {
			t.Fatalf("notice status = %q, want failed", sp.Status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no failure notice was pushed")
	}
}

func Test_Runner_StartBackground_requires_notifier(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		// no Notifier
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.StartBackground(context.Background(), subagent.Request{
		Task:   "no notifier",
		Parent: protocol.MessageAddress{ThreadID: "parent-1"},
	}); !errors.Is(err, subagent.ErrBackgroundUnsupported) {
		t.Fatalf("err = %v, want ErrBackgroundUnsupported", err)
	}
}

func Test_recordInboundSubagentStatus_flips_running_to_terminal(t *testing.T) {
	sink := newTurnWorldStateSink(2, 0, nil)
	// The spawn turn left the handle "running".
	sink.RecordSubagent("parent-1::sub::3", "reviewer", "running", "")
	// The completion notice's terminal SubagentPart arrives on the woken turn.
	sp, err := protocol.NewSubagentPart(protocol.SubagentPart{
		ID:       "parent-1::sub::3",
		Name:     "reviewer",
		ThreadID: "parent-1::sub::3",
		Status:   protocol.SubagentCompleted,
		Summary:  "done",
	})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}
	user := protocol.ChatMessage{Role: protocol.RoleUser, Content: []protocol.ContentPart{sp}}
	recordInboundSubagentStatus(sink, user)

	handles := sink.worldState().ActiveSubagents
	if len(handles) != 1 {
		t.Fatalf("handles = %d, want 1", len(handles))
	}
	if handles[0].Status != "completed" {
		t.Fatalf("status = %q, want completed (flipped from running)", handles[0].Status)
	}
	if handles[0].Summary != "done" {
		t.Fatalf("summary = %q, want done", handles[0].Summary)
	}
}

func Test_recordInboundSubagentStatus_ignores_nonterminal(t *testing.T) {
	sink := newTurnWorldStateSink(1, 0, nil)
	sp, err := protocol.NewSubagentPart(protocol.SubagentPart{ID: "t", ThreadID: "t", Status: protocol.SubagentRunning})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}
	user := protocol.ChatMessage{Content: []protocol.ContentPart{sp}}
	recordInboundSubagentStatus(sink, user)
	if got := len(sink.worldState().ActiveSubagents); got != 0 {
		t.Fatalf("non-terminal inbound part should record nothing, got %d handles", got)
	}
}

func Test_Runner_StartBackground_runs_multiple_under_cap(t *testing.T) {
	enq := newCaptureEnqueuer()
	r, err := NewRunner(Config{
		Store:                  &fakeStore{},
		Loader:                 fakeLoader{},
		Provider:               &fakeProvider{},
		Assembler:              contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Notifier:               enq,
		MaxBackgroundSubagents: 1, // force queueing through the semaphore
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		if _, err := r.StartBackground(context.Background(), subagent.Request{
			Task:   "bg work",
			Parent: protocol.MessageAddress{ThreadID: "parent-1"},
		}); err != nil {
			t.Fatalf("start background %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		select {
		case <-enq.done:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d/%d background children completed", i, n)
		}
	}
}
