package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func streamClient(t *testing.T, baseURL string) *SidecarClient {
	t.Helper()
	c, err := NewSidecarClient(SidecarConfig{BaseURL: baseURL, Format: FormatOpenAI})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
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
