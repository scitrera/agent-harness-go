// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sseServer(t *testing.T, chunks []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, c := range chunks {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
			if flusher != nil {
				flusher.Flush()
			}
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

func streamClient(t *testing.T, baseURL string) *OpenAICompatClient {
	t.Helper()
	c, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: baseURL, Format: FormatOpenAI})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

// usageStreamClient builds a client that opts into streamed usage capture.
func usageStreamClient(t *testing.T, baseURL string) *OpenAICompatClient {
	t.Helper()
	c, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: baseURL, Format: FormatOpenAI, StreamUsage: true})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

// TestChatStreamCapturesUsage verifies that with StreamUsage on, the trailing
// usage-only chunk (which arrives AFTER finish_reason and carries no choices) is
// read into ChatResponse.Usage + Model rather than being skipped by the
// finish_reason short-circuit.
func TestChatStreamCapturesUsage(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","model":"gpt-4o-mini","choices":[{"delta":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}`,
		`{"model":"gpt-4o-mini","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`,
	})
	defer srv.Close()

	resp, err := usageStreamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(DeltaKind, string) error { return nil })
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if resp.Usage.PromptTokens != 11 || resp.Usage.CompletionTokens != 3 || resp.Usage.TotalTokens != 14 {
		t.Fatalf("usage = %+v, want 11/3/14", resp.Usage)
	}
	if resp.Model != "gpt-4o-mini" {
		t.Fatalf("model = %q, want gpt-4o-mini", resp.Model)
	}
	tp, ok := resp.Message.Content[0].AsText()
	if !ok || tp.Text != "Hi" {
		t.Fatalf("final text = %q", tp.Text)
	}
}

// TestChatStreamDefaultDropsPostFinishUsage confirms the default (StreamUsage off)
// keeps the finish_reason short-circuit — so a post-finish usage chunk is NOT
// awaited (no regression to the [DONE]-less hang). Usage is simply zero.
func TestChatStreamDefaultDropsPostFinishUsage(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","choices":[{"delta":{"content":"Hi"},"finish_reason":"stop"}]}`,
		`{"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`,
	})
	defer srv.Close()

	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(DeltaKind, string) error { return nil })
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if resp.Usage.TotalTokens != 0 {
		t.Fatalf("usage total = %d, want 0 (finish_reason short-circuit before usage chunk)", resp.Usage.TotalTokens)
	}
}

func TestChatStreamSSEText(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","choices":[{"delta":{"role":"assistant","content":"Hel"}}]}`,
		`{"choices":[{"delta":{"content":"lo"}}]}`,
	})
	defer srv.Close()

	var got strings.Builder
	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(_ DeltaKind, text string) error {
		got.WriteString(text)
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if got.String() != "Hello" {
		t.Fatalf("streamed tokens = %q", got.String())
	}
	tp, ok := resp.Message.Content[0].AsText()
	if !ok || tp.Text != "Hello" {
		t.Fatalf("final text = %q", tp.Text)
	}
}

func TestChatStreamSSEToolCalls(t *testing.T) {
	srv := sseServer(t, []string{
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read_file","arguments":""}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"path\":"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"SOUL.md\"}"}}]}}]}`,
	})
	defer srv.Close()

	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(DeltaKind, string) error { return nil })
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(resp.Message.Content) != 1 {
		t.Fatalf("expected 1 tool_call part, got %d", len(resp.Message.Content))
	}
	tc, ok := resp.Message.Content[0].AsToolCall()
	if !ok || tc.ID != "call_1" || tc.Name != "read_file" {
		t.Fatalf("tool_call = %+v", tc)
	}
	if string(tc.Args["path"]) != `"SOUL.md"` {
		t.Fatalf("accumulated args = %v", tc.Args)
	}
}

// TestChatStreamTerminatesOnFinishReasonWithoutDONE reproduces the MLflow AI
// Gateway → Fireworks behavior: it emits a terminal finish_reason but NO
// `data: [DONE]` sentinel and holds the connection open (no prompt EOF). The
// client MUST end the stream on finish_reason; otherwise it blocks on the next
// read until the idle-liveness timer fires — the ~26-50s post-response hang.
func TestChatStreamTerminatesOnFinishReasonWithoutDONE(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, c := range []string{
			`{"id":"a1","choices":[{"finish_reason":null,"delta":{"role":"assistant","content":"Hi"}}]}`,
			`{"choices":[{"finish_reason":"stop","delta":{"content":null}}]}`,
		} {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-release // NO [DONE], NO close: hold the connection open like the real gateway.
	}))
	// LIFO: unblock the handler BEFORE httptest waits for it in Close().
	defer srv.Close()
	defer close(release)

	type result struct {
		resp ChatResponse
		text string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		var got strings.Builder
		resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(_ DeltaKind, text string) error {
			got.WriteString(text)
			return nil
		})
		ch <- result{resp, got.String(), err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("stream: %v", r.err)
		}
		if r.text != "Hi" {
			t.Fatalf("streamed tokens = %q, want Hi", r.text)
		}
		tp, ok := r.resp.Message.Content[0].AsText()
		if !ok || tp.Text != "Hi" {
			t.Fatalf("final text = %q", tp.Text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ChatStream did not terminate on finish_reason (blocked waiting for [DONE]/EOF)")
	}
}

func TestChatStreamFallsBackToJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"a1","choices":[{"message":{"role":"assistant","content":"non-stream"}}]}`)
	}))
	defer srv.Close()

	var got strings.Builder
	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(_ DeltaKind, text string) error {
		got.WriteString(text)
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	tp, _ := resp.Message.Content[0].AsText()
	if tp.Text != "non-stream" || got.String() != "non-stream" {
		t.Fatalf("fallback text=%q delta=%q", tp.Text, got.String())
	}
}

// nativeStreamClient builds a client on the sidecar (native) wire format, which
// bypasses the shared llm-protocol codec and parses SSE locally
// (parseSSEStream) — the production sandbox path.
func nativeStreamClient(t *testing.T, baseURL string) *OpenAICompatClient {
	t.Helper()
	c, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: baseURL, Format: FormatNative})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

// A reasoning model streams its thinking trace on a channel of its own
// (`reasoning_content`) ahead of the answer. Both channels must reach the
// consumer tagged and in order, and the assembled message must LEAD with the
// reasoning part — the turn layer dedups its reconstruction against these parts
// by (type, text), so assembled order has to mirror emitted order.
func TestChatStreamSSEReasoningContent(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","choices":[{"delta":{"role":"assistant","reasoning_content":"let me "}}]}`,
		`{"choices":[{"delta":{"reasoning_content":"think"}}]}`,
		`{"choices":[{"delta":{"content":"Hello"}}]}`,
	})
	defer srv.Close()

	var text, reasoning strings.Builder
	var kinds []DeltaKind
	resp, err := nativeStreamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(kind DeltaKind, s string) error {
		kinds = append(kinds, kind)
		if kind == DeltaReasoning {
			reasoning.WriteString(s)
		} else {
			text.WriteString(s)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if reasoning.String() != "let me think" || text.String() != "Hello" {
		t.Fatalf("streamed reasoning = %q, text = %q", reasoning.String(), text.String())
	}
	wantKinds := []DeltaKind{DeltaReasoning, DeltaReasoning, DeltaText}
	if len(kinds) != len(wantKinds) {
		t.Fatalf("delta kinds = %v, want %v", kinds, wantKinds)
	}
	for i, k := range wantKinds {
		if kinds[i] != k {
			t.Fatalf("delta kinds = %v, want %v", kinds, wantKinds)
		}
	}
	if len(resp.Message.Content) != 2 {
		t.Fatalf("expected [reasoning, text], got %#v", resp.Message.Content)
	}
	rt, ok := reasoningPartText(resp.Message.Content[0])
	if !ok || rt != "let me think" {
		t.Fatalf("final reasoning = %q (ok=%v)", rt, ok)
	}
	tp, ok := resp.Message.Content[1].AsText()
	if !ok || tp.Text != "Hello" {
		t.Fatalf("final text = %q", tp.Text)
	}
}

// Some upstreams name the same channel `reasoning` rather than
// `reasoning_content`; the local parser accepts both.
func TestChatStreamSSEReasoningAlias(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","choices":[{"delta":{"role":"assistant","reasoning":"hmm"}}]}`,
		`{"choices":[{"delta":{"content":"Hi"}}]}`,
	})
	defer srv.Close()

	var reasoning strings.Builder
	resp, err := nativeStreamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(kind DeltaKind, s string) error {
		if kind == DeltaReasoning {
			reasoning.WriteString(s)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if reasoning.String() != "hmm" {
		t.Fatalf("streamed reasoning = %q", reasoning.String())
	}
	rt, ok := reasoningPartText(resp.Message.Content[0])
	if !ok || rt != "hmm" {
		t.Fatalf("final reasoning = %q (ok=%v)", rt, ok)
	}
}

// The OpenAI-format path goes through the shared llm-protocol client, whose
// codec maps `reasoning_content` to a reasoning stream event: sharedChatStream
// must forward it on the reasoning channel rather than dropping it (it did
// until reasoning got its own channel).
func TestSharedChatStreamForwardsReasoningDeltas(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"a1","choices":[{"delta":{"role":"assistant","reasoning_content":"weighing options"}}]}`,
		`{"choices":[{"delta":{"content":"Hello"}}]}`,
	})
	defer srv.Close()

	var text, reasoning strings.Builder
	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(kind DeltaKind, s string) error {
		if kind == DeltaReasoning {
			reasoning.WriteString(s)
		} else {
			text.WriteString(s)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if reasoning.String() != "weighing options" || text.String() != "Hello" {
		t.Fatalf("streamed reasoning = %q, text = %q", reasoning.String(), text.String())
	}
	rt, ok := reasoningPartText(resp.Message.Content[0])
	if !ok || rt != "weighing options" {
		t.Fatalf("final reasoning = %q (ok=%v)", rt, ok)
	}
}
