package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// keepaliveInterval is how often the SSE stream emits a comment ping so idle
// connections (and intermediaries) stay open.
const keepaliveInterval = 15 * time.Second

// Server is the HTTP surface for the web UI: a REST/SSE API plus the embedded
// single-page app. It is transport glue only — turns run through the Channel and
// the turn runner wired in cmd.
//
// The server has NO authentication and is intended for localhost (loopback) use.
// State-changing endpoints carry a same-origin CSRF guard, but anyone able to
// reach the listen address can drive the agent — do not expose it beyond
// loopback without your own auth layer in front.
type Server struct {
	ch        *Channel
	store     *store.FileStore
	sessions  *Index
	canceller *turncancel.Canceller
	mux       *http.ServeMux
}

// New builds the HTTP server. canceller may be nil (the /api/cancel endpoint
// then reports nothing cancelled).
func New(ch *Channel, fs *store.FileStore, sessions *Index, canceller *turncancel.Canceller) *Server {
	s := &Server{ch: ch, store: fs, sessions: sessions, canceller: canceller, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the composed http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/sessions", s.handleListSessions)
	// State-changing endpoints are guarded by a same-origin check (CSRF defense).
	// Reads (GET/SSE/assets) are not guarded — they are not CSRF write vectors.
	s.mux.HandleFunc("POST /api/sessions", guardCSRF(s.handleCreateSession))
	s.mux.HandleFunc("DELETE /api/sessions/{id}", guardCSRF(s.handleDeleteSession))
	s.mux.HandleFunc("GET /api/sessions/{id}/history", s.handleHistory)
	s.mux.HandleFunc("POST /api/chat", guardCSRF(s.handleChat))
	s.mux.HandleFunc("GET /api/stream", s.handleStream)
	s.mux.HandleFunc("POST /api/cancel", guardCSRF(s.handleCancel))
	s.mux.HandleFunc("/", s.handleSPA)
}

// sameOriginOK implements the standard same-origin CSRF defense for browser
// clients: if the request carries an Origin header, its host:port must equal the
// request's Host. Requests without an Origin (non-browser clients like curl) are
// allowed — they are not a browser-CSRF vector. The bundled SPA is same-origin
// and so always passes.
func sameOriginOK(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // not a browser CSRF vector
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return u.Host == r.Host
}

// guardCSRF wraps a state-changing handler with the same-origin check, rejecting
// cross-origin browser requests with 403 before any side effect runs.
func guardCSRF(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !sameOriginOK(r) {
			writeError(w, http.StatusForbidden, fmt.Errorf("cross-origin request rejected"))
			return
		}
		next(w, r)
	}
}

func (s *Server) handleListSessions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.sessions.List())
}

func (s *Server) handleCreateSession(w http.ResponseWriter, _ *http.Request) {
	sess, err := s.sessions.Create()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, sess)
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.sessions.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// Clear the persisted transcript too (best-effort).
	_ = s.store.SaveHistory(r.Context(), id, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	msgs, err := s.store.LoadHistory(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if msgs == nil {
		msgs = []protocol.ChatMessage{}
	}
	writeJSON(w, http.StatusOK, msgs)
}

type chatRequest struct {
	ThreadID string `json:"thread_id"`
	Text     string `json:"text"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.ThreadID == "" || req.Text == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("thread_id and text are required"))
		return
	}
	taskID, err := randID("task-")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	part, err := protocol.NewTextPart(req.Text)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	addr := protocol.MessageAddress{ThreadID: req.ThreadID, TaskID: taskID}
	msg := protocol.ChatMessage{
		ID:      "user-" + taskID,
		Role:    protocol.RoleUser,
		Addr:    addr,
		Content: []protocol.ContentPart{part},
	}
	// Update the switcher (title-on-first-message + ordering) before dispatch.
	if err := s.sessions.Touch(req.ThreadID, req.Text); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.ch.Enqueue(r.Context(), channel.Inbound{Addr: addr, Message: msg}); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"task_id": taskID})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("thread_id")
	if threadID == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("thread_id is required"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering if any

	events, cancel := s.ch.Subscribe(threadID)
	defer cancel()

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ping := time.NewTicker(keepaliveInterval)
	defer ping.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-events:
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

type cancelRequest struct {
	TaskID string `json:"task_id"`
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var req cancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	cancelled := false
	if s.canceller != nil && req.TaskID != "" {
		cancelled = s.canceller.Cancel(req.TaskID)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"cancelled": cancelled})
}

// handleSPA serves the embedded single-page app for any non-API GET.
func (s *Server) handleSPA(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
