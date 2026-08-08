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

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/sessionlog"
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

func newResumableTestServer(t *testing.T) (*Server, *Channel, *store.FileStore, *sessionlog.RecordingPublisher) {
	t.Helper()
	dir := t.TempDir()
	fs := store.NewFileStore(dir, dir)
	idx, err := NewIndex(dir, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	ch := NewChannel()
	history, err := sessionlog.BindDefaultHistory(fs, "default")
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := sessionlog.NewStaticWorkspaceResolver("default", nil)
	if err != nil {
		t.Fatal(err)
	}
	events := sessionlog.NewMemoryEventLog(sessionlog.MemoryEventLogConfig{})
	coordinator, err := sessionlog.NewCoordinator(sessionlog.CoordinatorConfig{
		Workspaces: resolver,
		History:    history,
		Events:     events,
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := sessionlog.NewRecordingPublisher(sessionlog.RecordingPublisherConfig{
		Events:           events,
		Next:             ch,
		SessionEvents:    ch,
		DefaultWorkspace: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewWithSessionService(ch, fs, idx, nil, coordinator), ch, fs, publisher
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

func TestSessionAttachReturnsSnapshotAndReplay(t *testing.T) {
	srv, _, fs, publisher := newResumableTestServer(t)
	ctx := context.Background()
	part, _ := protocol.NewTextPart("persisted")
	persisted := protocol.ChatMessage{SchemaVersion: "1.0", ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if err := fs.SaveHistory(ctx, "t1", []protocol.ChatMessage{persisted}); err != nil {
		t.Fatal(err)
	}

	request := spec.NewSessionAttachRequest("t1", "client-1")
	initial := postSessionAttach(t, srv, request)
	if initial.WorkspaceID != "default" || len(initial.Snapshot.Messages) != 1 || initial.Snapshot.Messages[0].ID != persisted.ID {
		t.Fatalf("initial attach = %#v", initial)
	}
	if initial.Snapshot.Messages[0].Addr.WorkspaceID != "default" || initial.Snapshot.Messages[0].Addr.ThreadID != "t1" {
		t.Fatalf("normalized legacy message address = %#v", initial.Snapshot.Messages[0].Addr)
	}

	assistant := spec.NewChatMessage("assistant-1", spec.RoleAssistant)
	assistant.Content = []spec.ContentPart{spec.NewTextPart("live")}
	if err := publisher.PublishEvent(ctx, channel.Event{
		Type:    channel.EventMessageFinal,
		Addr:    protocol.MessageAddress{ThreadID: "t1"},
		Message: &assistant,
	}); err != nil {
		t.Fatal(err)
	}
	request.ResumeAfter = &initial.Snapshot.Cursor
	resumed := postSessionAttach(t, srv, request)
	if len(resumed.Snapshot.Messages) != 2 || resumed.Snapshot.Messages[1].ID != assistant.ID {
		t.Fatalf("resumed snapshot messages = %#v", resumed.Snapshot.Messages)
	}
	if resumed.Replay == nil || resumed.Replay.Status != spec.SessionReplayComplete || len(resumed.Replay.Events) != 1 {
		t.Fatalf("resumed replay = %#v", resumed.Replay)
	}
	if resumed.Replay.Through.Generation != resumed.Snapshot.Cursor.Generation || resumed.Replay.Through.Sequence != resumed.Snapshot.Cursor.Sequence {
		t.Fatalf("replay boundary = %#v; snapshot = %#v", resumed.Replay.Through, resumed.Snapshot.Cursor)
	}
}

func TestSessionStreamEmitsCursorBearingLiveEvent(t *testing.T) {
	srv, _, _, publisher := newResumableTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/session/stream?session_id=t1&client_id=client-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("session stream connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session stream status = %d", resp.StatusCode)
	}

	message := spec.NewChatMessage("assistant-1", spec.RoleAssistant)
	message.Content = []spec.ContentPart{spec.NewTextPart("streamed")}
	if err := publisher.PublishEvent(ctx, channel.Event{
		Type:    channel.EventMessageFinal,
		Addr:    protocol.MessageAddress{ThreadID: "t1"},
		Message: &message,
	}); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(resp.Body)
	wantData := false
	var live spec.SessionEvent
	for scanner.Scan() {
		line := scanner.Text()
		if line == "event: session_event" {
			wantData = true
			continue
		}
		if wantData && strings.HasPrefix(line, "data: ") {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &live); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if live.WorkspaceID != "default" || live.SessionID != "t1" || live.Cursor.Sequence != 1 || live.Kind != spec.SessionEventChatStream {
		t.Fatalf("live session event = %#v", live)
	}
}

func TestDeleteSessionResetsGenerationAndDropsLiveProjection(t *testing.T) {
	srv, _, fs, publisher := newResumableTestServer(t)
	session, err := srv.sessions.Create()
	if err != nil {
		t.Fatal(err)
	}
	message := spec.NewChatMessage("assistant-1", spec.RoleAssistant)
	message.Content = []spec.ContentPart{spec.NewTextPart("old answer")}
	if err := fs.SaveHistory(context.Background(), session.ID, []protocol.ChatMessage{message}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishEvent(context.Background(), channel.Event{
		Type:    channel.EventMessageFinal,
		Addr:    protocol.MessageAddress{ThreadID: session.ID},
		Message: &message,
	}); err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest(session.ID, "client-1")
	before := postSessionAttach(t, srv, request)
	if before.Snapshot.Cursor.Sequence != 1 || len(before.Snapshot.Messages) != 1 {
		t.Fatalf("before delete = %#v", before)
	}

	recorder := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/sessions/"+session.ID, nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	after := postSessionAttach(t, srv, request)
	if after.Snapshot.Cursor.Generation == before.Snapshot.Cursor.Generation || after.Snapshot.Cursor.Sequence != 0 {
		t.Fatalf("after delete cursor = %#v; before = %#v", after.Snapshot.Cursor, before.Snapshot.Cursor)
	}
	if len(after.Snapshot.Messages) != 0 {
		t.Fatalf("after delete messages = %#v", after.Snapshot.Messages)
	}
}

func TestSessionStreamQueryValidatesVersionAndCursor(t *testing.T) {
	tests := []string{
		"/api/session/stream?session_id=t1&client_id=c1&protocol_version=2",
		"/api/session/stream?session_id=t1&client_id=c1&schema_revision=invalid",
		"/api/session/stream?session_id=t1&client_id=c1&generation=g1",
		"/api/session/stream?session_id=t1&client_id=c1&sequence=1",
		"/api/session/stream?session_id=t1&client_id=c1&generation=g1&sequence=9007199254740992",
	}
	for _, target := range tests {
		t.Run(target, func(t *testing.T) {
			if _, err := sessionAttachRequestFromQuery(httptest.NewRequest(http.MethodGet, target, nil)); err == nil {
				t.Fatal("expected invalid query to be rejected")
			}
		})
	}
}

func postSessionAttach(t *testing.T, server *Server, request spec.SessionAttachRequest) spec.SessionAttachResult {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/session/attach", strings.NewReader(string(body))))
	if recorder.Code != http.StatusOK {
		t.Fatalf("attach status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var result spec.SessionAttachResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
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
	defer func() { _ = resp.Body.Close() }()
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

func TestHandleRenameSession(t *testing.T) {
	srv, _, _ := newTestServer(t)
	sess, err := srv.sessions.Create()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	req := httptest.NewRequest(http.MethodPatch, "/api/sessions/"+sess.ID, strings.NewReader(`{"title":"Q3 revenue analysis"}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	list := srv.sessions.List()
	if len(list) != 1 || list[0].Title != "Q3 revenue analysis" {
		t.Fatalf("title not updated: %+v", list)
	}
}

func TestHandleRenameSessionRejectsEmptyTitle(t *testing.T) {
	srv, _, _ := newTestServer(t)
	sess, _ := srv.sessions.Create()

	req := httptest.NewRequest(http.MethodPatch, "/api/sessions/"+sess.ID, strings.NewReader(`{"title":"   "}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (title must be non-empty)", rec.Code)
	}
}

func TestHandleRenameSessionRejectsCrossOrigin(t *testing.T) {
	srv, _, _ := newTestServer(t)
	sess, _ := srv.sessions.Create()

	req := httptest.NewRequest(http.MethodPatch, "/api/sessions/"+sess.ID, strings.NewReader(`{"title":"evil"}`))
	req.Header.Set("Origin", "http://evil.example")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusNoContent {
		t.Fatal("cross-origin rename should be rejected by the CSRF guard")
	}
	if got := srv.sessions.List()[0].Title; got == "evil" {
		t.Fatalf("cross-origin rename mutated state: title=%q", got)
	}
}
