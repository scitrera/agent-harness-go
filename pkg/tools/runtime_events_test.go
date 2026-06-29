package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

type recordingToolEventSink struct {
	events []ToolEvent
}

func (s *recordingToolEventSink) EmitToolEvent(_ context.Context, event ToolEvent) error {
	s.events = append(s.events, event)
	return nil
}

func Test_Registry_Invoke_emits_started_and_finished_events_with_safe_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := reg.Register("secret_tool", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	args := json.RawMessage(`{"token":"super-secret"}`)

	// When
	_, err := reg.Invoke(ctx, Request{CallID: "call-1", Name: "secret_tool", Arguments: args})

	// Then
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+finished events, got %#v", sink.events)
	}
	sum := sha256.Sum256(args)
	wantHash := hex.EncodeToString(sum[:])
	if sink.events[0].Status != ToolEventStarted || sink.events[1].Status != ToolEventFinished {
		t.Fatalf("unexpected statuses: %#v", sink.events)
	}
	for _, event := range sink.events {
		if event.CallID != "call-1" || event.ToolName != "secret_tool" {
			t.Fatalf("unexpected event identity: %#v", event)
		}
		if event.ArgumentsHash != wantHash {
			t.Fatalf("argument hash = %q, want %q", event.ArgumentsHash, wantHash)
		}
		raw, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			t.Fatalf("marshal event: %v", marshalErr)
		}
		if string(raw) == string(args) || jsonContains(raw, "super-secret") {
			t.Fatalf("event leaked raw argument data: %s", string(raw))
		}
	}
	if sink.events[1].DurationMS <= 0 {
		t.Fatalf("expected finished duration metadata, got %#v", sink.events[1])
	}
}

func Test_Registry_Invoke_emits_failed_event_for_malformed_local_args(t *testing.T) {
	// Given
	ctx := context.Background()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	args := json.RawMessage(`{"path":"secret-value"`)

	// When
	_, err = reg.Invoke(ctx, Request{CallID: "bad-json", Name: "read_file", Arguments: args})

	// Then
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument error, got %v", err)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+failed events, got %#v", sink.events)
	}
	failed := sink.events[1]
	if failed.Status != ToolEventFinished || !failed.IsError || failed.ErrorMessage == "" {
		t.Fatalf("expected failed event metadata, got %#v", failed)
	}
	raw, marshalErr := json.Marshal(failed)
	if marshalErr != nil {
		t.Fatalf("marshal failed event: %v", marshalErr)
	}
	if jsonContains(raw, "secret-value") {
		t.Fatalf("event leaked malformed raw arguments: %s", string(raw))
	}
}

func Test_Registry_Invoke_emits_failed_event_without_valid_argument_leak(t *testing.T) {
	// Given
	ctx := context.Background()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	secretPath := "valid-secret-token-123.txt"
	args := json.RawMessage(`{"path":"` + secretPath + `"}`)

	// When
	_, err = reg.Invoke(ctx, Request{CallID: "missing-secret-path", Name: "read_file", Arguments: args})

	// Then
	if err == nil {
		t.Fatal("expected read_file error")
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+failed events, got %#v", sink.events)
	}
	failed := sink.events[1]
	if failed.Status != ToolEventFinished || !failed.IsError || failed.ErrorMessage == "" {
		t.Fatalf("expected failed event metadata, got %#v", failed)
	}
	raw, marshalErr := json.Marshal(failed)
	if marshalErr != nil {
		t.Fatalf("marshal failed event: %v", marshalErr)
	}
	if jsonContains(raw, secretPath) {
		t.Fatalf("event leaked valid raw arguments: %s", string(raw))
	}
}

func Test_Registry_Invoke_preserves_shell_failure_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, MaxOutput: 64}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	args := json.RawMessage(`{"command":"sh","args":["-c","printf shell-partial; exit 7"],"max_output":64}`)

	// When
	result, err := reg.Invoke(ctx, Request{CallID: "shell-failure", Name: "shell", Arguments: args})

	// Then
	if err == nil {
		t.Fatal("expected shell failure")
	}
	if result.Metadata.ExitCode != 7 || result.Metadata.PID <= 0 {
		t.Fatalf("expected failed result metadata, got %#v", result.Metadata)
	}
	if result.Metadata.OutputBytes != len("shell-partial") || result.Metadata.OutputTruncated {
		t.Fatalf("unexpected output metadata: %#v", result.Metadata)
	}
	if !jsonContains(result.Payload, "shell-partial") {
		t.Fatalf("expected partial output in payload, got %s", string(result.Payload))
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+failed events, got %#v", sink.events)
	}
	failed := sink.events[1]
	if failed.Result.ExitCode != 7 || failed.Result.PID <= 0 || failed.Result.OutputBytes != len("shell-partial") {
		t.Fatalf("failed event lost command metadata: %#v", failed)
	}
}

func Test_Registry_Invoke_preserves_python_timeout_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, Python: "sh"}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	args := json.RawMessage(`{"code":"printf abcdef; sleep 1","timeout_ms":10,"max_output":3}`)

	// When
	result, err := reg.Invoke(ctx, Request{CallID: "python-timeout", Name: "python", Arguments: args})

	// Then
	if err == nil {
		t.Fatal("expected python timeout")
	}
	if result.Metadata.ExitCode != -1 || result.Metadata.PID <= 0 {
		t.Fatalf("expected timeout result metadata, got %#v", result.Metadata)
	}
	if result.Metadata.OutputBytes != len("abcdef") || !result.Metadata.OutputTruncated {
		t.Fatalf("expected truncated output metadata, got %#v", result.Metadata)
	}
	if !jsonContains(result.Payload, "abc") {
		t.Fatalf("expected partial output in payload, got %s", string(result.Payload))
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+failed events, got %#v", sink.events)
	}
	failed := sink.events[1]
	if failed.Result.ExitCode != -1 || failed.Result.PID <= 0 || failed.Result.OutputBytes != len("abcdef") || !failed.Result.OutputTruncated {
		t.Fatalf("failed event lost timeout metadata: %#v", failed)
	}
}

func Test_Registry_Invoke_emits_failed_event_when_handler_errors(t *testing.T) {
	// Given
	ctx := context.Background()
	sink := &recordingToolEventSink{}
	reg := NewRegistry()
	reg.SetEventSink(sink)
	if err := reg.Register("bad", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		return Result{}, errors.New("boom")
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	_, err := reg.Invoke(ctx, Request{CallID: "call-err", Name: "bad", Arguments: json.RawMessage(`{}`)})

	// Then
	if err == nil {
		t.Fatal("expected handler error")
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected started+failed events, got %#v", sink.events)
	}
	if sink.events[1].Status != ToolEventFinished || !sink.events[1].IsError || sink.events[1].ErrorMessage == "" {
		t.Fatalf("expected finished error event, got %#v", sink.events[1])
	}
}

func jsonContains(raw []byte, needle string) bool {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return false
	}
	return strings.Contains(string(encoded), needle)
}
