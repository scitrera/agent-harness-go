package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/steering"
)

func steeringInbound(id, thread string, mark bool) channel.Inbound {
	msg := protocol.ChatMessage{ID: id, Role: protocol.RoleUser}
	if mark {
		msg = steering.Mark(msg)
	}
	addr := protocol.MessageAddress{ThreadID: thread, TaskID: "task-" + id}
	msg.Addr = addr
	return channel.Inbound{Addr: addr, Message: msg}
}

func shellSteeringInbound(t *testing.T, id, thread string, triggerAgent bool) channel.Inbound {
	t.Helper()
	inbound := steeringInbound(id, thread, true)
	if err := shellcontext.Put(&inbound.Message, shellcontext.Record{Command: "pwd", CWD: "/work", ExitCode: 0, TriggerAgent: triggerAgent}); err != nil {
		t.Fatal(err)
	}
	return inbound
}

// gatedExecutor announces that its first turn has begun, then blocks until
// released. runTask opens the steering lane before calling the executor, so
// `started` is the signal that the lane is genuinely open — without it the test
// would race the dispatcher goroutine and park into a lane that does not exist
// yet (which in production degrades safely to an ordinary turn, but makes for a
// flaky test).
type gatedExecutor struct {
	mu      sync.Mutex
	seen    []string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *gatedExecutor) Run(_ context.Context, _ protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
	e.once.Do(func() {
		close(e.started)
		<-e.release
	})
	e.mu.Lock()
	e.seen = append(e.seen, user.ID)
	e.mu.Unlock()
	return protocol.ChatMessage{ID: "a-" + user.ID, Role: protocol.RoleAssistant}, nil
}

// gatedSource holds back everything from gateAfter onward until gate is closed,
// so a test can guarantee a message is fetched only once a turn is in flight.
type gatedSource struct {
	mu        sync.Mutex
	items     []channel.Inbound
	i         int
	gateAfter int
	gate      chan struct{}
}

func (s *gatedSource) FetchTask(ctx context.Context) (channel.Inbound, error) {
	s.mu.Lock()
	idx := s.i
	s.mu.Unlock()
	if idx >= s.gateAfter {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return channel.Inbound{}, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.i >= len(s.items) {
		return channel.Inbound{}, channel.ErrNoTask
	}
	e := s.items[s.i]
	s.i++
	return e, nil
}

func (s *gatedSource) consumed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.i
}

func (e *gatedExecutor) turns() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.seen...)
}

func Test_Loop_parks_a_steering_send_instead_of_starting_a_turn(t *testing.T) {
	// Given: a turn already running on the thread, and a marked steering send
	// arriving behind it.
	exec := &gatedExecutor{started: make(chan struct{}), release: make(chan struct{})}
	source := &gatedSource{
		items: []channel.Inbound{
			steeringInbound("u1", "t1", false),
			steeringInbound("s1", "t1", true),
		},
		gateAfter: 1,
		gate:      make(chan struct{}),
	}
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	inbox := steering.New()
	runner.SetSteering(inbox)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan LoopStats, 1)
	go func() {
		stats, _ := runner.RunLoop(ctx, LoopConfig{Concurrency: 4, PollInterval: time.Millisecond})
		done <- stats
	}()

	// Release the steering send only once the first turn is genuinely in flight.
	select {
	case <-exec.started:
	case <-ctx.Done():
		t.Fatal("first turn never started")
	}
	close(source.gate)

	deadline := time.Now().Add(3 * time.Second)
	for source.consumed() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// It must be parked into the running turn's lane, not dispatched.
	parked := inbox.Drain(steering.Key("", "t1"))
	close(exec.release)
	cancel()
	<-done

	// Then
	if len(parked) != 1 || parked[0].ID != "s1" {
		t.Fatalf("parked = %#v, want the steering send delivered into the running turn", parked)
	}
	// It must NOT have run as a turn of its own.
	for _, id := range exec.turns() {
		if id == "s1" {
			t.Fatal("the steering send started a turn of its own instead of joining the running one")
		}
	}
}

func Test_Loop_runs_an_unmarked_message_as_its_own_turn(t *testing.T) {
	// Given: the same shape, but the sender did NOT declare a steering send. A
	// message that merely arrives during a busy period is still its own turn and
	// must not be swallowed into an unrelated one.
	exec := &gatedExecutor{started: make(chan struct{}), release: make(chan struct{})}
	close(exec.release)
	source := &queueSource{items: []channel.Inbound{
		steeringInbound("u1", "t1", false),
		steeringInbound("u2", "t1", false),
	}}
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	runner.SetSteering(steering.New())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stats, err := runner.RunLoop(ctx, LoopConfig{Concurrency: 4, PollInterval: time.Millisecond, MaxTurns: 2})
	if err != nil {
		t.Fatalf("loop: %v", err)
	}

	// Then
	if stats.Turns != 2 {
		t.Fatalf("turns = %d, want 2 — unmarked messages each get their own turn", stats.Turns)
	}
}

// recordingRejector captures what the sender would be told.
type recordingRejector struct {
	mu       sync.Mutex
	rejected []string
	reasons  []string
}

func (r *recordingRejector) RejectSteering(_ context.Context, in channel.Inbound, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejected = append(r.rejected, in.Message.ID)
	r.reasons = append(r.reasons, reason)
}

func (r *recordingRejector) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.rejected...), append([]string(nil), r.reasons...)
}

func Test_RunOnce_rejects_steering_stranded_by_a_finished_turn(t *testing.T) {
	// Given: a message parked so late that the turn had already taken its last
	// drain. It cannot do what the user asked, and running it as its own turn
	// would be a different action than the one requested.
	inbox := steering.New()
	exec := execFunc(func(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
		if user.ID == "u1" {
			inbox.Park(steering.Key(addr.WorkspaceID, addr.ThreadID), steeringInbound("late", "t1", true).Message)
		}
		return protocol.ChatMessage{ID: "a-" + user.ID, Role: protocol.RoleAssistant}, nil
	})
	source := &queueSource{items: []channel.Inbound{steeringInbound("u1", "t1", false)}}
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	rejector := &recordingRejector{}
	runner.SetSteering(inbox)
	runner.SetSteeringRejector(rejector)

	// When: the turn runs and closes its lane.
	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("turn: %v", err)
	}

	// Then: the sender is told, rather than the message quietly becoming a turn.
	rejected, reasons := rejector.snapshot()
	if len(rejected) != 1 || rejected[0] != "late" {
		t.Fatalf("rejected = %v, want the stranded message", rejected)
	}
	if reasons[0] != reasonTurnEndedFirst {
		t.Fatalf("reason = %q, want the turn-ended reason", reasons[0])
	}
}

func Test_RunOnce_rejects_a_steering_send_with_no_turn_running(t *testing.T) {
	// Given: the serial loop, where nothing can be in flight when FetchTask
	// returns — so an interjection has nothing to join.
	inbox := steering.New()
	var ran []string
	exec := execFunc(func(_ context.Context, _ protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
		ran = append(ran, user.ID)
		return protocol.ChatMessage{ID: "a-" + user.ID, Role: protocol.RoleAssistant}, nil
	})
	source := &queueSource{items: []channel.Inbound{
		steeringInbound("s1", "t1", true),
		steeringInbound("u1", "t1", false),
	}}
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	rejector := &recordingRejector{}
	runner.SetSteering(inbox)
	runner.SetSteeringRejector(rejector)

	// When
	assistant, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	// Then: the steering send is rejected and RunOnce moves on to the real turn.
	rejected, reasons := rejector.snapshot()
	if len(rejected) != 1 || rejected[0] != "s1" {
		t.Fatalf("rejected = %v, want the steering send", rejected)
	}
	if reasons[0] != reasonNoActiveTurn {
		t.Fatalf("reason = %q, want the no-active-turn reason", reasons[0])
	}
	if assistant.ID != "a-u1" {
		t.Fatalf("assistant = %q, want the ordinary turn to have run", assistant.ID)
	}
	for _, id := range ran {
		if id == "s1" {
			t.Fatal("a rejected steering send was executed as a turn anyway")
		}
	}
}

func Test_RunOnceFallsBackShellSteeringSoContextIsNotLost(t *testing.T) {
	var ran []protocol.ChatMessage
	exec := execFunc(func(_ context.Context, _ protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
		ran = append(ran, user)
		return protocol.ChatMessage{ID: "a-" + user.ID, Role: protocol.RoleAssistant}, nil
	})
	source := &queueSource{items: []channel.Inbound{shellSteeringInbound(t, "shell", "t1", false)}}
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatal(err)
	}
	rejector := &recordingRejector{}
	runner.SetSteering(steering.New())
	runner.SetSteeringRejector(rejector)

	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 1 || ran[0].ID != "shell" || steering.IsRequested(ran[0]) {
		t.Fatalf("fallback turns = %#v", ran)
	}
	if _, ok := shellcontext.FromMessage(ran[0]); !ok {
		t.Fatal("fallback lost structured shell context")
	}
	if rejected, _ := rejector.snapshot(); len(rejected) != 0 {
		t.Fatalf("shell fallback was rejected: %v", rejected)
	}
}

func Test_RunOnceFallsBackShellSteeringStrandedAtTurnEnd(t *testing.T) {
	inbox := steering.New()
	var ran []string
	exec := execFunc(func(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
		ran = append(ran, user.ID)
		if user.ID == "u1" {
			inbox.Park(steering.Key(addr.WorkspaceID, addr.ThreadID), shellSteeringInbound(t, "shell-late", "t1", true).Message)
		}
		return protocol.ChatMessage{ID: "a-" + user.ID, Role: protocol.RoleAssistant}, nil
	})
	runner, err := NewRunner(&queueSource{items: []channel.Inbound{steeringInbound("u1", "t1", false)}}, exec)
	if err != nil {
		t.Fatal(err)
	}
	runner.SetSteering(inbox)
	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 2 || ran[0] != "u1" || ran[1] != "shell-late" {
		t.Fatalf("turns = %v", ran)
	}
}

func Test_SetSteering_adopts_a_rejecting_task_source(t *testing.T) {
	// A transport that can talk back to the sender should be used without the
	// host having to wire it a second time.
	source := &rejectingSource{}
	exec := execFunc(func(context.Context, protocol.MessageAddress, protocol.ChatMessage) (protocol.ChatMessage, error) {
		return protocol.ChatMessage{}, nil
	})
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	runner.SetSteering(steering.New())
	if runner.rejector == nil {
		t.Fatal("a task source implementing SteeringRejector was not adopted")
	}
}

// rejectingSource is a task source that can also report rejections.
type rejectingSource struct {
	queueSource
	recordingRejector
}

func Test_classifySteering_distinguishes_the_three_outcomes(t *testing.T) {
	source := &queueSource{}
	exec := execFunc(func(context.Context, protocol.MessageAddress, protocol.ChatMessage) (protocol.ChatMessage, error) {
		return protocol.ChatMessage{}, nil
	})
	runner, err := NewRunner(source, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// No inbox wired: steering is inert, so everything is an ordinary turn.
	if got := runner.classifySteering(steeringInbound("s1", "t1", true)); got != notSteering {
		t.Fatalf("outcome = %v, want notSteering with no inbox wired", got)
	}

	inbox := steering.New()
	runner.SetSteering(inbox)

	// Marked but nothing running → missed (and therefore rejected upstream),
	// never silently promoted to a turn of its own.
	if got := runner.classifySteering(steeringInbound("s1", "t1", true)); got != steeringMissed {
		t.Fatalf("outcome = %v, want steeringMissed with no turn running", got)
	}

	defer inbox.Begin(steering.Key("", "t1"))()

	if got := runner.classifySteering(steeringInbound("s2", "t1", true)); got != steeringParked {
		t.Fatalf("outcome = %v, want steeringParked", got)
	}
	// Unmarked, same lane → still its own turn.
	if got := runner.classifySteering(steeringInbound("u2", "t1", false)); got != notSteering {
		t.Fatalf("outcome = %v, want notSteering for an unmarked message", got)
	}
}

func Test_steering_marker_survives_json_round_trip(t *testing.T) {
	// The marker crosses the wire as message meta, so it has to survive
	// encode/decode — a marker lost in transit silently becomes a new turn.
	marked := steering.Mark(protocol.ChatMessage{ID: "s1", Role: protocol.RoleUser})
	raw, err := json.Marshal(marked)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded protocol.ChatMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !steering.IsRequested(decoded) {
		t.Fatalf("steering marker did not survive the wire: %s", raw)
	}
}
