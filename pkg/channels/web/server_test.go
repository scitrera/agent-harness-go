package web

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
)

func newTestServer(t *testing.T) (*Server, *Channel, *store.FileStore) {
	t.Helper()
	dir := t.TempDir()
	fs := store.NewFileStore(dir, dir)
	idx, err := NewIndex(dir, fixedClock())
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	ch := NewChannel()
	return New(ch, fs, idx, nil), ch, fs
}

func TestHandleChatEnqueuesAndTouchesSession(t *testing.T) {
	srv, ch, _ := newTestServer(t)

	body := strings.NewReader(`{"thread_id":"t1","text":"hello agent"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/chat", body)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["task_id"] == "" {
		t.Fatal("expected task_id in response")
	}

	in, err := ch.FetchTask(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if in.Addr.ThreadID != "t1" {
		t.Fatalf("thread mismatch: %s", in.Addr.ThreadID)
	}
	got, ok := in.Message.Content[0].AsText()
	if !ok || got.Text != "hello agent" {
		t.Fatalf("message text mismatch: %+v", in.Message.Content)
	}
	if in.Addr.TaskID != resp["task_id"] {
		t.Fatalf("task id mismatch: %s vs %s", in.Addr.TaskID, resp["task_id"])
	}

	// Session index should now carry the thread with a derived title.
	if list := srv.sessions.List(); len(list) != 1 || list[0].Title != "hello agent" {
		t.Fatalf("session not touched correctly: %+v", list)
	}
}

func TestHandleChatRejectsCrossOrigin(t *testing.T) {
	srv, ch, _ := newTestServer(t)

	body := strings.NewReader(`{"thread_id":"t1","text":"hello agent"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/chat", body)
	// httptest sets Host to example.com; an Origin pointing elsewhere is cross-origin.
	req.Header.Set("Origin", "http://evil.example:1234")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body=%s (want 403)", rec.Code, rec.Body.String())
	}
	// Side effect must NOT have happened: nothing enqueued, no session touched.
	fctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := ch.FetchTask(fctx); err == nil {
		t.Fatal("cross-origin request enqueued a task; side effect should have been blocked")
	}
	if list := srv.sessions.List(); len(list) != 0 {
		t.Fatalf("cross-origin request touched a session: %+v", list)
	}
}

func TestHandleChatAllowsSameOrigin(t *testing.T) {
	srv, ch, _ := newTestServer(t)

	body := strings.NewReader(`{"thread_id":"t1","text":"hello agent"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/chat", body)
	// Origin host matches the request Host (example.com) -> same-origin -> allowed.
	req.Header.Set("Origin", "http://"+req.Host)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s (want 202)", rec.Code, rec.Body.String())
	}
	if _, err := ch.FetchTask(context.Background()); err != nil {
		t.Fatalf("same-origin request did not enqueue a task: %v", err)
	}
}

func TestHandleChatAllowsNoOrigin(t *testing.T) {
	// curl-style client with no Origin header must still work (back-compat).
	srv, ch, _ := newTestServer(t)

	body := strings.NewReader(`{"thread_id":"t1","text":"hello agent"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/chat", body)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s (want 202)", rec.Code, rec.Body.String())
	}
	if _, err := ch.FetchTask(context.Background()); err != nil {
		t.Fatalf("no-Origin request did not enqueue a task: %v", err)
	}
}

func TestCreateSessionRejectsCrossOrigin(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", nil)
	req.Header.Set("Origin", "http://evil.example")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d (want 403)", rec.Code)
	}
	// No session should have been created.
	if list := srv.sessions.List(); len(list) != 0 {
		t.Fatalf("cross-origin create made a session: %+v", list)
	}
}

func TestGetUnaffectedByOriginCheck(t *testing.T) {
	// Reads must not be subject to the same-origin guard even with a foreign Origin.
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	req.Header.Set("Origin", "http://evil.example")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d (want 200); read should be unguarded", rec.Code)
	}
}

func TestHandleHistoryRoundTrip(t *testing.T) {
	srv, _, fs := newTestServer(t)
	part, _ := protocol.NewTextPart("persisted reply")
	msgs := []protocol.ChatMessage{{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}
	if err := fs.SaveHistory(context.Background(), "t1", msgs); err != nil {
		t.Fatalf("save: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/t1/history", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var out []protocol.ChatMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 message, got %d", len(out))
	}
	got, ok := out[0].Content[0].AsText()
	if !ok || got.Text != "persisted reply" {
		t.Fatalf("history text mismatch: %+v", out[0])
	}
}

func TestHandleSessionsCRUD(t *testing.T) {
	srv, _, _ := newTestServer(t)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/sessions", nil))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d", rec.Code)
	}
	var created Session
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if created.ID == "" {
		t.Fatal("no id returned")
	}

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/sessions", nil))
	var list []Session
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("list mismatch: %+v", list)
	}

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/sessions/"+created.ID, nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", rec.Code)
	}
}

func TestStreamEmitsPublishedEvent(t *testing.T) {
	srv, ch, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/stream?thread_id=t1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream connect: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d", resp.StatusCode)
	}

	// Give the handler a moment to register the subscriber, then publish.
	deadline := time.Now().Add(time.Second)
	for {
		ch.mu.RLock()
		n := len(ch.subs["t1"])
		ch.mu.RUnlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = ch.PublishEvent(context.Background(), channel.Event{
		Type:  channel.EventTokenDelta,
		Addr:  protocol.MessageAddress{ThreadID: "t1"},
		Delta: "streamed-token",
	})

	sc := bufio.NewScanner(resp.Body)
	found := false
	for sc.Scan() {
		if strings.Contains(sc.Text(), "streamed-token") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("did not observe published event on the SSE stream")
	}
}
