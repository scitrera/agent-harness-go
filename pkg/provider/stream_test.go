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
			fmt.Fprintf(w, "data: %s\n\n", c)
			if flusher != nil {
				flusher.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
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

	resp, err := usageStreamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(string) error { return nil })
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

	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(string) error { return nil })
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
	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(text string) error {
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

	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(string) error { return nil })
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
			fmt.Fprintf(w, "data: %s\n\n", c)
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
		resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(text string) error {
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
		fmt.Fprint(w, `{"id":"a1","choices":[{"message":{"role":"assistant","content":"non-stream"}}]}`)
	}))
	defer srv.Close()

	var got strings.Builder
	resp, err := streamClient(t, srv.URL).ChatStream(context.Background(), ChatRequest{}, func(text string) error {
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
