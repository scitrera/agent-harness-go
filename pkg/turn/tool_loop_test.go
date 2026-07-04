package turn

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type scriptedProvider struct {
	responses []provider.ChatResponse
	requests  []provider.ChatRequest
}

func (p *scriptedProvider) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return provider.ChatResponse{}, err
	}
	p.requests = append(p.requests, req)
	if len(p.responses) == 0 {
		return provider.ChatResponse{}, errors.New("no scripted response")
	}
	resp := p.responses[0]
	p.responses = p.responses[1:]
	return resp, nil
}

// streamingScriptedProvider is a scriptedProvider that also satisfies
// StreamingProvider: for the Nth call it streams streamText[N] (when non-empty)
// through onDelta before returning the Nth scripted response. Used to exercise
// the production streaming path, where the answer text reaches the stream via
// token_delta and must NOT be appended a second time at finalize.
type streamingScriptedProvider struct {
	scriptedProvider
	streamText []string
}

func (p *streamingScriptedProvider) ChatStream(ctx context.Context, req provider.ChatRequest, onDelta provider.DeltaFunc) (provider.ChatResponse, error) {
	idx := len(p.requests)
	if idx < len(p.streamText) && p.streamText[idx] != "" {
		if err := onDelta(p.streamText[idx]); err != nil {
			return provider.ChatResponse{}, err
		}
	}
	return p.Chat(ctx, req)
}

// On the streaming path the model's answer text is delivered via token_delta, so
// finalize must NOT re-append it: the finalized message is exactly the
// reconstruction of the streamed events — tool_call, tool_result, then the one
// streamed text part (spec §7) — with no duplicate.
func Test_Runner_Run_streaming_finalized_message_has_no_duplicate_answer_text(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"recorded":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{"value":1}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &streamingScriptedProvider{
		scriptedProvider: scriptedProvider{responses: []provider.ChatResponse{
			{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
			{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
		}},
		// First call emits the tool_call (no text); the second streams "done".
		streamText: []string{"", "done"},
	}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
		Streaming:         true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please record")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run turn: %v", err)
	}

	// Finalized message carries exactly the full turn, with a single text part.
	if len(assistant.Content) != 3 ||
		assistant.Content[0].Type() != protocol.ContentToolCall ||
		assistant.Content[1].Type() != protocol.ContentToolResult ||
		assistant.Content[2].Type() != protocol.ContentText {
		t.Fatalf("expected [tool_call, tool_result, text], got %#v", assistant.Content)
	}
	if txt, _ := assistant.Content[2].AsText(); txt.Text != "done" {
		t.Fatalf("expected streamed answer text 'done', got %q", txt.Text)
	}
	texts := 0
	for _, p := range assistant.Content {
		if p.Type() == protocol.ContentText {
			texts++
		}
	}
	if texts != 1 {
		t.Fatalf("streamed answer text must not be duplicated at finalize, got %d text parts", texts)
	}
	// The streamed text rode as token_delta (not a second part_appended at
	// finalize), so the egress carries exactly one text part_appended.
	textAppends := 0
	for _, ev := range publisher.events {
		if ev.Type == channel.EventPartAppended && ev.Part != nil && ev.Part.Type() == protocol.ContentText {
			textAppends++
		}
	}
	if textAppends != 1 {
		t.Fatalf("expected exactly one text part_appended (the streamed answer), got %d", textAppends)
	}
}

func Test_Runner_Run_invokes_tool_call_and_reprompts_provider(t *testing.T) {
	// Given
	ctx := context.Background()
	store := &fakeStore{}
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"recorded":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{"value":1}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please record")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	// When
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})

	// Then
	if err != nil {
		t.Fatalf("run turn: %v", err)
	}
	// The returned message is the canonical finalized message: it carries the
	// stream id (started/appended/finalized share one id) and the full
	// reconstruction of the turn — tool_call, tool_result, then the answer text —
	// not just the model's trailing reply (spec §7).
	if assistant.ID != streamMessageID(protocol.MessageAddress{ThreadID: "thread-1"}) {
		t.Fatalf("expected finalized stream id, got %q", assistant.ID)
	}
	if len(assistant.Content) != 3 ||
		assistant.Content[0].Type() != protocol.ContentToolCall ||
		assistant.Content[1].Type() != protocol.ContentToolResult ||
		assistant.Content[2].Type() != protocol.ContentText {
		t.Fatalf("expected [tool_call, tool_result, text] in finalized message, got %#v", assistant.Content)
	}
	if txt, _ := assistant.Content[2].AsText(); txt.Text != "done" {
		t.Fatalf("expected final answer text 'done', got %q", txt.Text)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("expected two provider requests, got %d", len(provider.requests))
	}
	if len(store.messages) != 4 {
		t.Fatalf("expected user, assistant tool call, tool result, final assistant; got %#v", store.messages)
	}
	if store.messages[2].Role != protocol.RoleToolResult {
		t.Fatalf("expected tool result message at index 2, got %#v", store.messages[2])
	}
	// Streamed content lifecycle. The provider here is non-streaming, so the
	// answer text isn't streamed via token_delta during the loop; finalize
	// appends it as a part_appended so the finalized message equals the
	// reconstruction of the streamed events: started, tool_call, tool_result,
	// text, final.
	streamEvents := make([]protocol.ContentPart, 0)
	contentEvents := make([]channel.Event, 0, len(publisher.events))
	for _, event := range publisher.events {
		if event.Type != "tool_lifecycle" {
			contentEvents = append(contentEvents, event)
		}
		if event.Part != nil {
			streamEvents = append(streamEvents, *event.Part)
		}
	}
	if len(contentEvents) != 5 {
		t.Fatalf("expected started, tool_call, tool_result, text, final; got %#v", contentEvents)
	}
	if contentEvents[0].Type != "message_started" {
		t.Fatalf("event[0] should be message_started: %#v", contentEvents[0])
	}
	if contentEvents[1].Type != "part_appended" || contentEvents[1].Part == nil || contentEvents[1].Part.Type() != protocol.ContentToolCall {
		t.Fatalf("event[1] should be a tool_call part_appended: %#v", contentEvents[1])
	}
	if contentEvents[2].Type != "part_appended" || contentEvents[2].Part == nil || contentEvents[2].Part.Type() != protocol.ContentToolResult {
		t.Fatalf("event[2] should be a tool_result part_appended: %#v", contentEvents[2])
	}
	if contentEvents[3].Type != "part_appended" || contentEvents[3].Part == nil || contentEvents[3].Part.Type() != protocol.ContentText {
		t.Fatalf("event[3] should be the answer-text part_appended: %#v", contentEvents[3])
	}
	if contentEvents[4].Type != "message_final" {
		t.Fatalf("event[4] should be message_final: %#v", contentEvents[4])
	}
	if last := contentEvents[4]; last.Message == nil || len(last.Message.Content) != 3 {
		t.Fatalf("finalized message must carry the full turn (tool_call, tool_result, text): %#v", last.Message)
	}
	if len(streamEvents) != 3 {
		t.Fatalf("expected streamed tool_call, tool_result and answer-text parts, got %#v", streamEvents)
	}
}

// A tool that returns extra Result.Parts (e.g. a subagent reference part) must
// have those parts co-located ON THE SAME tool-result message as the tool_result
// part, and the enclosing assistant message id must reach the tool as
// Request.MessageID (so back-refs can record the parent MESSAGE id).
func Test_Runner_Run_records_extra_result_parts_on_tool_result_message(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	registry := tools.NewRegistry()
	var seenMessageID string
	if err := registry.Register("delegate", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		seenMessageID = req.MessageID
		res, err := tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
		if err != nil {
			return tools.Result{}, err
		}
		sp, perr := protocol.NewSubagentPart(protocol.SubagentPart{
			ID:       req.CallID,
			Name:     "child",
			ThreadID: "thread-1::sub::1",
			Status:   protocol.SubagentCompleted,
			Summary:  "child summary",
		})
		if perr != nil {
			return tools.Result{}, perr
		}
		res.Parts = []protocol.ContentPart{sp}
		return res, nil
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "delegate"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser}); err != nil {
		t.Fatalf("run turn: %v", err)
	}

	// The tool saw the spawning assistant message id (the tool-call message).
	if seenMessageID != "assistant-tool" {
		t.Fatalf("Request.MessageID = %q, want assistant-tool", seenMessageID)
	}
	// The persisted tool-result message co-locates [tool_result, subagent].
	if len(store.messages) < 3 || store.messages[2].Role != protocol.RoleToolResult {
		t.Fatalf("expected a tool-result message at index 2, got %#v", store.messages)
	}
	trMsg := store.messages[2]
	if len(trMsg.Content) != 2 ||
		trMsg.Content[0].Type() != protocol.ContentToolResult ||
		trMsg.Content[1].Type() != protocol.ContentSubagent {
		t.Fatalf("expected [tool_result, subagent] on the tool-result message, got %#v", trMsg.Content)
	}
	sp, ok := trMsg.Content[1].AsSubagent()
	if !ok || sp.ThreadID != "thread-1::sub::1" || sp.Status != protocol.SubagentCompleted {
		t.Fatalf("subagent part not recorded correctly: %+v (ok=%v)", sp, ok)
	}
}

func Test_Runner_Run_stops_when_tool_loop_exceeds_limit(t *testing.T) {
	// Given
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool-1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-tool-2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{}),
		MaxToolIterations: 1,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser})

	// Then
	if !errors.Is(err, ErrToolLoopLimit) {
		t.Fatalf("expected ErrToolLoopLimit, got %v", err)
	}
}

// A tool that fails to invoke (here a handler error standing in for the real
// "not pre-authorized" registry denial) must NOT abort the turn: the error is
// handed back to the model as a tool_result, and the model then produces a
// normal reply. Aborting would end the turn with no assistant message, which
// reads to the UI as a hang.
func Test_Runner_Run_failed_tool_call_is_returned_to_model_not_aborted(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("shell", tools.HandlerFunc(func(context.Context, tools.Request) (tools.Result, error) {
		return tools.Result{}, errors.New("approval required: shell: tool is not pre-authorized")
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("I can't run shell here.")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser})
	if err != nil {
		t.Fatalf("turn must complete, not abort on tool error: %v", err)
	}
	// The finalized message carries the stream id and the full turn — the
	// tool_call, the error tool_result, then the model's recovery reply.
	if assistant.ID != streamMessageID(protocol.MessageAddress{ThreadID: "thread-1"}) {
		t.Fatalf("expected finalized stream id, got %q", assistant.ID)
	}
	if txt, _ := assistant.Content[len(assistant.Content)-1].AsText(); txt.Text != "I can't run shell here." {
		t.Fatalf("expected the model's recovery reply as the trailing text, got %q", txt.Text)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("expected provider reprompted (2 requests) after the tool error, got %d", len(provider.requests))
	}
	sawToolResult := false
	for _, ev := range publisher.events {
		if ev.Type == "part_appended" && ev.Part != nil && ev.Part.Type() == protocol.ContentToolResult {
			sawToolResult = true
		}
	}
	if !sawToolResult {
		t.Fatal("expected a tool_result error part streamed to the UI")
	}
}

// Test_Runner_Run_failed_turn_finalizes_with_reason asserts that a turn which
// FAILS on a live context (here: the tool-loop limit) still publishes a terminal
// message_final event carrying the failure reason in meta.error — symmetric with
// the cancel path — so a client streaming the reply lane sees the message end
// instead of spinning until its own timeout. Regression for the gap where the
// failure branch returned the error with no finalize (reason reached only the log
// + the Aether FailTask).
func Test_Runner_Run_failed_turn_finalizes_with_reason(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"recorded":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{"value":1}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	// The model never stops calling tools, so the loop hits MaxToolIterations=1
	// and returns ErrToolLoopLimit on the second iteration.
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool-1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-tool-2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
	}}
	publisher := &fakePublisher{}
	store := &fakeStore{}
	runner, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 1,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please record")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})

	// The turn errors with the loop-limit sentinel (so the task layer FailTasks)...
	if !errors.Is(err, ErrToolLoopLimit) {
		t.Fatalf("expected ErrToolLoopLimit, got %v", err)
	}
	// ...but the returned message is the finalized partial, annotated with the reason.
	if finalized.ID != streamMessageID(protocol.MessageAddress{ThreadID: "thread-1"}) {
		t.Fatalf("expected finalized stream id, got %q", finalized.ID)
	}
	raw, ok := finalized.Meta[metaErrorKey]
	if !ok {
		t.Fatalf("expected meta[%q] on failed-turn finalize, meta=%v", metaErrorKey, finalized.Meta)
	}
	var reason string
	if err := json.Unmarshal(raw, &reason); err != nil || reason != ErrToolLoopLimit.Error() {
		t.Fatalf("expected meta.error = %q, got %q (err %v)", ErrToolLoopLimit.Error(), reason, err)
	}
	if _, cancelled := finalized.Meta[metaCancelledKey]; cancelled {
		t.Fatalf("a failed turn must not be marked cancelled")
	}
	// A terminal message_final event reached the reply lane, carrying the reason.
	var final *channel.Event
	for i := range publisher.events {
		if publisher.events[i].Type == channel.EventMessageFinal {
			final = &publisher.events[i]
		}
	}
	if final == nil {
		t.Fatal("expected a terminal message_final event on the failed turn")
	}
	if final.Message == nil {
		t.Fatal("message_final event carried no message")
	}
	if _, ok := final.Message.Meta[metaErrorKey]; !ok {
		t.Fatalf("message_final must carry meta.error, meta=%v", final.Message.Meta)
	}
	// The partial turn was committed to history (so the failure persists on reload).
	if len(store.messages) == 0 {
		t.Fatal("expected the failed turn to be committed to the store")
	}
}

// Test_Runner_Run_streams_tool_extra_parts_into_finalized_assistant asserts that
// a tool's co-located extra parts (result.Parts — e.g. spawn_subagent's
// SubagentPart carrying the child thread_id) are streamed and therefore folded
// into the finalized assistant, so the end-of-turn memory commit (which writes
// the finalized assistant, not the tool-result message) carries the subagent
// linkage. Regression for Gap A: previously only the tool_result part was
// streamed, so the SubagentPart never reached MemoryLayer and parent→child
// survived only via the "::sub::" thread-name convention.
func Test_Runner_Run_streams_tool_extra_parts_into_finalized_assistant(t *testing.T) {
	ctx := context.Background()
	const childThread = "thread-1::sub::1"
	registry := tools.NewRegistry()
	if err := registry.Register("delegate", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		res, err := tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"thread_id":"`+childThread+`"}`))
		if err != nil {
			return tools.Result{}, err
		}
		sp, perr := protocol.NewSubagentPart(protocol.SubagentPart{
			ID: req.CallID, Name: "researcher", ThreadID: childThread,
			Status: protocol.SubagentCompleted, Summary: "did research",
		})
		if perr != nil {
			return tools.Result{}, perr
		}
		res.Parts = []protocol.ContentPart{sp}
		return res, nil
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "delegate", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	mem := &fakeMemory{}
	runner, err := NewRunner(Config{
		Store:                         &fakeStore{},
		Loader:                        fakeLoader{},
		Registry:                      registry,
		Provider:                      provider,
		Publisher:                     &fakePublisher{},
		Assembler:                     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations:             2,
		Memory:                        mem,
		MemoryAutoCommit:              true,
		MemoryAutoCommitAssistantOnly: true, // production sahara setting
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please delegate")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run turn: %v", err)
	}

	// The finalized assistant carries the SubagentPart (folded in from the stream).
	findSubagent := func(m protocol.ChatMessage) bool {
		for _, p := range m.Content {
			if sp, ok := p.AsSubagent(); ok && sp.ThreadID == childThread {
				return true
			}
		}
		return false
	}
	if !findSubagent(finalized) {
		t.Fatalf("finalized assistant missing SubagentPart(thread=%s): %#v", childThread, finalized.Content)
	}
	// And the end-of-turn memory commit (assistant-only) carried it — so parent→child
	// linkage reaches MemoryLayer, not just the thread-name convention.
	if len(mem.appended) != 1 || len(mem.appended[0]) != 1 {
		t.Fatalf("expected one assistant-only commit, got %#v", mem.appended)
	}
	if !findSubagent(mem.appended[0][0]) {
		t.Fatalf("committed assistant missing SubagentPart(thread=%s): %#v", childThread, mem.appended[0][0].Content)
	}
}
