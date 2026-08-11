package memorylayer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// fakeServer is a minimal stand-in for MemoryLayer's chat API, including its
// response envelopes ({"thread": …}, {"threads": […]}, {"messages": […]}).
type fakeServer struct {
	t          *testing.T
	threads    map[string]map[string]any
	messages   map[string][]map[string]any
	appends    int
	deletes    int
	created    int
	workspaces map[string]bool
	url        string
}

func newFakeServer(t *testing.T) (*fakeServer, *httptest.Server) {
	t.Helper()
	f := &fakeServer{
		t:          t,
		threads:    map[string]map[string]any{},
		messages:   map[string][]map[string]any{},
		workspaces: map[string]bool{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	f.url = srv.URL
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	segments := strings.Split(strings.Trim(path, "/"), "/")
	w.Header().Set("content-type", "application/json")

	switch {
	case segments[0] == "workspaces" && len(segments) == 2 && r.Method == http.MethodGet:
		if !f.workspaces[segments[1]] {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workspace": map[string]any{"id": segments[1]}})

	case segments[0] == "workspaces" && len(segments) == 1 && r.Method == http.MethodPost:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		id, _ := body["id"].(string)
		f.workspaces[id] = true
		_ = json.NewEncoder(w).Encode(map[string]any{"workspace": map[string]any{"id": id}})

	case segments[0] == "threads" && len(segments) == 1 && r.Method == http.MethodPost:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		// The real server MINTS the id and ignores one supplied by the client.
		f.created++
		id := fmt.Sprintf("thread_server_%d", f.created)
		workspaceID, _ := body["workspace_id"].(string)
		title, _ := body["title"].(string)
		thread := map[string]any{"id": id, "workspace_id": workspaceID, "title": title, "created_at": "2026-08-07T10:00:00Z", "updated_at": "2026-08-07T10:00:00Z"}
		f.threads[fakeThreadKey(workspaceID, id)] = thread
		_ = json.NewEncoder(w).Encode(map[string]any{"thread": thread})

	case segments[0] == "threads" && len(segments) == 1 && r.Method == http.MethodGet:
		list := make([]map[string]any, 0, len(f.threads))
		workspaceID := r.URL.Query().Get("workspace_id")
		for _, thread := range f.threads {
			if thread["workspace_id"] == workspaceID {
				list = append(list, thread)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"threads": list})

	case len(segments) == 2 && segments[0] == "threads" && r.Method == http.MethodGet:
		key := fakeThreadKey(r.URL.Query().Get("workspace_id"), segments[1])
		thread, ok := f.threads[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"thread": thread})

	case len(segments) == 2 && segments[0] == "threads" && r.Method == http.MethodPut:
		key := fakeThreadKey(r.URL.Query().Get("workspace_id"), segments[1])
		thread, ok := f.threads[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if title, ok := body["title"].(string); ok {
			thread["title"] = title
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"thread": thread})

	case len(segments) == 2 && segments[0] == "threads" && r.Method == http.MethodDelete:
		f.deletes++
		key := fakeThreadKey(r.URL.Query().Get("workspace_id"), segments[1])
		delete(f.threads, key)
		delete(f.messages, key)
		_ = json.NewEncoder(w).Encode(map[string]any{"deleted": true})

	case len(segments) == 4 && segments[0] == "threads" && segments[2] == "messages" && r.Method == http.MethodDelete:
		key := fakeThreadKey(r.URL.Query().Get("workspace_id"), segments[1])
		kept := f.messages[key][:0]
		for _, m := range f.messages[key] {
			if m["id"] != segments[3] {
				kept = append(kept, m)
			}
		}
		f.messages[key] = kept
		_ = json.NewEncoder(w).Encode(map[string]any{"deleted": true})

	case len(segments) == 3 && segments[0] == "threads" && segments[2] == "messages" && r.Method == http.MethodPost:
		// The real server auto-creates the thread, and that insert fails a
		// foreign-key constraint when the workspace does not exist — surfaced
		// as an opaque 500.
		if !f.workspaces[r.URL.Query().Get("workspace_id")] {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Failed to append messages"})
			return
		}
		workspaceID := r.URL.Query().Get("workspace_id")
		key := fakeThreadKey(workspaceID, segments[1])
		f.appends++
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		// Appending to an unknown thread id creates it server-side.
		if _, ok := f.threads[key]; !ok {
			f.threads[key] = map[string]any{
				"id": segments[1], "title": segments[1],
				"workspace_id": workspaceID,
				"created_at":   "2026-08-07T10:00:00Z", "updated_at": "2026-08-07T10:00:00Z",
			}
		}
		for i, m := range body.Messages {
			m["id"] = fmt.Sprintf("msg_%s_%d", segments[1], len(f.messages[key])+i)
			f.messages[key] = append(f.messages[key], m)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": body.Messages})

	case len(segments) == 3 && segments[0] == "threads" && segments[2] == "messages" && r.Method == http.MethodGet:
		key := fakeThreadKey(r.URL.Query().Get("workspace_id"), segments[1])
		if _, ok := f.threads[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": f.messages[key]})

	default:
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
	}
}

func fakeThreadKey(workspaceID, threadID string) string {
	if workspaceID == "default" {
		return threadID
	}
	return workspaceID + "\x00" + threadID
}

func newTestStore(t *testing.T) (*Store, *fakeServer) {
	t.Helper()
	fake, srv := newFakeServer(t)
	s, err := New(Config{BaseURL: srv.URL, Workspace: "default"})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// Startup does this for real; without it every write 500s.
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return s, fake
}

func userMessage(t *testing.T, id, text string) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
}

// The harness hands over the FULL transcript every turn while MemoryLayer's API
// appends. Without a diff each turn would re-append everything, so the second
// save must send only what is new.
func TestSaveHistoryAppendsOnlyNewMessages(t *testing.T) {
	s, fake := newTestStore(t)
	ctx := context.Background()

	first := []protocol.ChatMessage{userMessage(t, "m1", "hello")}
	if err := s.SaveHistory(ctx, "thread-1", first); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	second := append(append([]protocol.ChatMessage{}, first...), userMessage(t, "m2", "again"))
	if err := s.SaveHistory(ctx, "thread-1", second); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}

	if got := len(fake.messages["thread-1"]); got != 2 {
		t.Fatalf("stored %d messages, want 2 (no re-append)", got)
	}
	// Re-saving an unchanged transcript must not call the server at all.
	appendsBefore := fake.appends
	if err := s.SaveHistory(ctx, "thread-1", second); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	if fake.appends != appendsBefore {
		t.Fatalf("appends = %d, want unchanged at %d for an unchanged transcript", fake.appends, appendsBefore)
	}
}

func TestConcurrentSaveDoesNotDuplicateOneTranscript(t *testing.T) {
	store, fake := newTestStore(t)
	messages := []protocol.ChatMessage{userMessage(t, "m1", "hello")}
	const writers = 32
	errors := make(chan error, writers)
	var wait sync.WaitGroup
	for i := 0; i < writers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errors <- store.SaveWorkspaceHistory(context.Background(), "default", "shared", messages)
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := len(fake.messages["shared"]); got != 1 {
		t.Fatalf("stored %d duplicate messages", got)
	}
}

// A round trip must preserve the message identity and content the UI renders.
func TestLoadHistoryRoundTrip(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	written := []protocol.ChatMessage{userMessage(t, "m1", "hello"), userMessage(t, "m2", "world")}
	if err := s.SaveHistory(ctx, "thread-1", written); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	got, err := s.LoadHistory(ctx, "thread-1")
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("loaded %d messages, want 2", len(got))
	}
	for i, msg := range got {
		if msg.ID != written[i].ID {
			t.Fatalf("message %d id = %q, want %q", i, msg.ID, written[i].ID)
		}
		text, ok := msg.Content[0].AsText()
		if !ok {
			t.Fatalf("message %d lost its text part", i)
		}
		wantText, _ := written[i].Content[0].AsText()
		if text.Text != wantText.Text {
			t.Fatalf("message %d text = %q, want %q", i, text.Text, wantText.Text)
		}
	}
}

func TestWorkspaceHistoryAndThreadCachesAreIsolated(t *testing.T) {
	fake, server := newFakeServer(t)
	store, err := New(Config{BaseURL: server.URL, Workspace: "project-a"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	messageA := userMessage(t, "same-message", "project A")
	messageB := userMessage(t, "same-message", "project B")
	if err := store.SaveWorkspaceHistory(ctx, "project-a", "shared", []protocol.ChatMessage{messageA}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkspaceHistory(ctx, "project-b", "shared", []protocol.ChatMessage{messageB}); err != nil {
		t.Fatal(err)
	}
	if len(fake.messages[fakeThreadKey("project-a", "shared")]) != 1 || len(fake.messages[fakeThreadKey("project-b", "shared")]) != 1 {
		t.Fatalf("workspace messages = %#v", fake.messages)
	}

	loadedA, err := store.LoadWorkspaceHistory(ctx, "project-a", "shared")
	if err != nil {
		t.Fatal(err)
	}
	loadedB, err := store.LoadWorkspaceHistory(ctx, "project-b", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if messageText(loadedA) != "project A" || messageText(loadedB) != "project B" {
		t.Fatalf("loaded A=%q B=%q", messageText(loadedA), messageText(loadedB))
	}

	if err := store.RefreshWorkspace(ctx, "project-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.RefreshWorkspace(ctx, "project-b"); err != nil {
		t.Fatal(err)
	}
	if got := store.ListWorkspace("project-a"); len(got) != 1 || got[0].ID != "shared" {
		t.Fatalf("project A threads = %#v", got)
	}
	if got := store.ListWorkspace("project-b"); len(got) != 1 || got[0].ID != "shared" {
		t.Fatalf("project B threads = %#v", got)
	}
	if err := store.RenameWorkspaceThread("project-b", "shared", "project B title"); err != nil {
		t.Fatal(err)
	}
	if got := store.ListWorkspace("project-a"); len(got) != 1 || got[0].Title != "shared" {
		t.Fatalf("project B rename changed project A cache: %#v", got)
	}
	if got := store.ListWorkspace("project-b"); len(got) != 1 || got[0].Title != "project B title" {
		t.Fatalf("project B title = %#v", got)
	}

	if err := store.DeleteWorkspaceHistory(ctx, "project-b", "shared"); err != nil {
		t.Fatal(err)
	}
	if len(fake.messages[fakeThreadKey("project-a", "shared")]) != 1 || len(fake.messages[fakeThreadKey("project-b", "shared")]) != 0 {
		t.Fatalf("workspace clear crossed boundaries: %#v", fake.messages)
	}
	if err := store.DeleteWorkspaceThread("project-b", "shared"); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.threads[fakeThreadKey("project-a", "shared")]; !ok {
		t.Fatal("deleting project B removed project A's same-ID thread")
	}
}

func messageText(messages []protocol.ChatMessage) string {
	if len(messages) != 1 || len(messages[0].Content) != 1 {
		return ""
	}
	part, ok := messages[0].Content[0].AsText()
	if !ok {
		return ""
	}
	return part.Text
}

// Loading tells the store what the remote already holds, so a save after a load
// must not re-append the loaded messages — the case a fresh process hits when it
// attaches to an existing thread.
func TestSaveAfterLoadDoesNotDuplicate(t *testing.T) {
	s, fake := newTestStore(t)
	ctx := context.Background()
	if err := s.SaveHistory(ctx, "thread-1", []protocol.ChatMessage{userMessage(t, "m1", "hello")}); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}

	// A second process attaching to the same thread.
	fresh, err := New(Config{BaseURL: fake.url, Workspace: "default"})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := fresh.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	loaded, err := fresh.LoadHistory(ctx, "thread-1")
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if err := fresh.SaveHistory(ctx, "thread-1", loaded); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	if got := len(fake.messages["thread-1"]); got != 1 {
		t.Fatalf("stored %d messages, want 1 (loaded messages re-appended)", got)
	}
}

// "Clear" means an empty conversation, not a vanished one: the thread must
// survive so the switcher still lists it.
func TestDeleteHistoryKeepsTheThread(t *testing.T) {
	s, fake := newTestStore(t)
	ctx := context.Background()
	if err := s.SaveHistory(ctx, "thread-1", []protocol.ChatMessage{userMessage(t, "m1", "hello")}); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	if err := s.DeleteHistory(ctx, "thread-1"); err != nil {
		t.Fatalf("DeleteHistory: %v", err)
	}
	if len(fake.messages["thread-1"]) != 0 {
		t.Fatalf("messages survived the clear: %+v", fake.messages["thread-1"])
	}
	if _, ok := fake.threads["thread-1"]; !ok {
		t.Fatal("clear removed the thread; it must survive so the switcher still lists it")
	}
	if fake.deletes != 0 {
		t.Fatalf("clear deleted the thread (%d thread deletes); it should delete messages only", fake.deletes)
	}
	// After a clear the same message may legitimately be written again.
	if err := s.SaveHistory(ctx, "thread-1", []protocol.ChatMessage{userMessage(t, "m1", "hello")}); err != nil {
		t.Fatalf("SaveHistory after clear: %v", err)
	}
	if got := len(fake.messages["thread-1"]); got != 1 {
		t.Fatalf("post-clear write stored %d messages, want 1", got)
	}
}

// A thread MemoryLayer has never seen is empty, not an error: the first turn of
// a new thread reads before it writes.
func TestLoadHistoryUnknownThreadIsEmpty(t *testing.T) {
	s, _ := newTestStore(t)
	got, err := s.LoadHistory(context.Background(), "never-seen")
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("loaded %d messages for an unknown thread, want 0", len(got))
	}
}

func TestThreadIndexCreateRenameListDelete(t *testing.T) {
	s, _ := newTestStore(t)

	session, err := s.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !strings.HasPrefix(session.ID, "thread_server_") {
		t.Fatalf("session id = %q, want the SERVER-minted id", session.ID)
	}
	if err := s.Rename(session.ID, "renamed thread"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	listed := s.List()
	if len(listed) != 1 || listed[0].Title != "renamed thread" {
		t.Fatalf("List = %+v, want one renamed thread", listed)
	}
	if err := s.Delete(session.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := s.List(); len(got) != 0 {
		t.Fatalf("List after delete = %+v, want empty", got)
	}
}

// Touch derives a title from the first user message, like the filesystem index.
func TestTouchDerivesTitle(t *testing.T) {
	s, _ := newTestStore(t)
	session, err := s.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Touch(session.ID, "what is the capital of France?"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	listed := s.List()
	if len(listed) != 1 {
		t.Fatalf("List = %+v", listed)
	}
	if listed[0].Title != "what is the capital of France?" {
		t.Fatalf("title = %q, want the first user message", listed[0].Title)
	}
}

// Refresh loads what another process created, which is the point of a shared
// store.
func TestRefreshPicksUpRemoteThreads(t *testing.T) {
	s, fake := newTestStore(t)
	fake.threads["thread-remote"] = map[string]any{
		"id": "thread-remote", "title": "made elsewhere",
		"workspace_id": "default",
		"created_at":   "2026-08-07T10:00:00Z", "updated_at": "2026-08-07T10:00:00Z",
	}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	listed := s.List()
	if len(listed) != 1 || listed[0].ID != "thread-remote" {
		t.Fatalf("List = %+v, want the remotely-created thread", listed)
	}
}

func TestLookupWorkspaceThreadReadsThroughAndCaches(t *testing.T) {
	s, fake := newTestStore(t)
	fake.threads[fakeThreadKey("project-a", "scheduled-thread")] = map[string]any{
		"id": "scheduled-thread", "title": "Nightly review",
		"workspace_id": "project-a",
		"created_at":   "2026-08-07T10:00:00Z", "updated_at": "2026-08-07T11:00:00Z",
	}

	got, found, err := s.LookupWorkspaceThread(context.Background(), "project-a", "scheduled-thread")
	if err != nil || !found {
		t.Fatalf("lookup = %+v found=%v err=%v", got, found, err)
	}
	if got.ID != "scheduled-thread" || got.Title != "Nightly review" {
		t.Fatalf("lookup = %+v", got)
	}
	if listed := s.ListWorkspace("project-a"); len(listed) != 1 || listed[0] != got {
		t.Fatalf("cached threads = %+v", listed)
	}

	missing, found, err := s.LookupWorkspaceThread(context.Background(), "project-a", "not-created-yet")
	if err != nil || found || missing.ID != "" {
		t.Fatalf("missing lookup = %+v found=%v err=%v", missing, found, err)
	}
}

// A first write must lazily ensure its addressed workspace. Multi-workspace
// hosts cannot pre-refresh every project they may receive after startup.
func TestFirstWriteCreatesMissingWorkspace(t *testing.T) {
	fake, srv := newFakeServer(t)
	s, err := New(Config{BaseURL: srv.URL, Workspace: "brand-new"})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()

	if err := s.SaveHistory(ctx, "t1", []protocol.ChatMessage{userMessage(t, "m1", "hi")}); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if !fake.workspaces["brand-new"] {
		t.Fatal("first write did not create the missing workspace")
	}
}
