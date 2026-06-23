package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type scriptedSourceResult struct {
	envelope channel.Inbound
	err      error
}

type scriptedSource struct {
	results []scriptedSourceResult
	calls   int
}

func (s *scriptedSource) FetchTask(_ context.Context) (channel.Inbound, error) {
	if s.calls >= len(s.results) {
		return channel.Inbound{}, channel.ErrNoTask
	}
	result := s.results[s.calls]
	s.calls++
	return result.envelope, result.err
}

func Test_Runner_RunLoop_executes_tasks_until_max_turns(t *testing.T) {
	// Given
	ctx := context.Background()
	source := &scriptedSource{results: []scriptedSourceResult{
		{envelope: taskEnvelope(t, "thread-1", "m1")},
		{envelope: taskEnvelope(t, "thread-1", "m2")},
	}}
	executor := &fakeExecutor{}
	runner, err := NewRunner(source, executor)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	stats, err := runner.RunLoop(ctx, LoopConfig{MaxTurns: 2})

	// Then
	if err != nil {
		t.Fatalf("run loop: %v", err)
	}
	if stats.Turns != 2 || stats.Errors != 0 || source.calls != 2 {
		t.Fatalf("unexpected stats=%#v calls=%d", stats, source.calls)
	}
}

func Test_Runner_RunLoop_continues_after_no_task(t *testing.T) {
	// Given
	ctx := context.Background()
	source := &scriptedSource{results: []scriptedSourceResult{
		{err: channel.ErrNoTask},
		{envelope: taskEnvelope(t, "thread-1", "m1")},
	}}
	runner, err := NewRunner(source, &fakeExecutor{})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	stats, err := runner.RunLoop(ctx, LoopConfig{PollInterval: time.Nanosecond, MaxTurns: 1})

	// Then
	if err != nil {
		t.Fatalf("run loop: %v", err)
	}
	if stats.Turns != 1 || stats.Errors != 0 || source.calls != 2 {
		t.Fatalf("unexpected stats=%#v calls=%d", stats, source.calls)
	}
}

func Test_Runner_RunLoop_backs_off_after_transient_error(t *testing.T) {
	// Given
	ctx := context.Background()
	transient := errors.New("transient")
	source := &scriptedSource{results: []scriptedSourceResult{
		{err: transient},
		{envelope: taskEnvelope(t, "thread-1", "m1")},
	}}
	runner, err := NewRunner(source, &fakeExecutor{})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	stats, err := runner.RunLoop(ctx, LoopConfig{ErrorBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond, MaxTurns: 1})

	// Then
	if err != nil {
		t.Fatalf("run loop: %v", err)
	}
	if stats.Turns != 1 || stats.Errors != 1 || source.calls != 2 {
		t.Fatalf("unexpected stats=%#v calls=%d", stats, source.calls)
	}
}

func taskEnvelope(t *testing.T, threadID string, messageID string) channel.Inbound {
	t.Helper()
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return channel.Inbound{
		Addr: protocol.MessageAddress{ThreadID: threadID},
		Message: protocol.ChatMessage{
			ID:      messageID,
			Role:    protocol.RoleUser,
			Content: []protocol.ContentPart{part},
		},
	}
}
