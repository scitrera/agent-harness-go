// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
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
	store     HistoryStore
	sessions  *Index
	canceller *turncancel.Canceller
	session   SessionService
	mux       *http.ServeMux
}

// HistoryStore is the transcript surface used by the web REST handlers.
type HistoryStore interface {
	LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error)
	DeleteHistory(ctx context.Context, threadID string) error
}

// SessionService is the spec-native attach/reset surface supplied by the host.
// It stays optional so embedders using only the legacy REST/SSE API are
// unaffected.
type SessionService interface {
	Attach(ctx context.Context, request spec.SessionAttachRequest) (spec.SessionAttachResult, error)
	ResetSession(ctx context.Context, requestedWorkspace, sessionID string) (spec.SessionCursor, error)
}

// New builds the HTTP server. canceller may be nil (the /api/cancel endpoint
// then reports nothing cancelled).
func New(ch *Channel, fs HistoryStore, sessions *Index, canceller *turncancel.Canceller) *Server {
	return NewWithSessionService(ch, fs, sessions, canceller, nil)
}

// NewWithSessionService builds the server with the resumable session API.
func NewWithSessionService(ch *Channel, fs HistoryStore, sessions *Index, canceller *turncancel.Canceller, session SessionService) *Server {
	s := &Server{ch: ch, store: fs, sessions: sessions, canceller: canceller, session: session, mux: http.NewServeMux()}
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
	s.mux.HandleFunc("PATCH /api/sessions/{id}", guardCSRF(s.handleRenameSession))
	s.mux.HandleFunc("GET /api/sessions/{id}/history", s.handleHistory)
	s.mux.HandleFunc("POST /api/chat", guardCSRF(s.handleChat))
	s.mux.HandleFunc("GET /api/stream", s.handleStream)
	s.mux.HandleFunc("POST /api/session/attach", guardCSRF(s.handleSessionAttach))
	s.mux.HandleFunc("GET /api/session/stream", s.handleSessionStream)
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
	if s.session != nil {
		if _, err := s.session.ResetSession(r.Context(), "", id); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	if err := s.sessions.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.store.DeleteHistory(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSessionAttach(w http.ResponseWriter, r *http.Request) {
	if s.session == nil {
		writeError(w, http.StatusNotImplemented, fmt.Errorf("resumable sessions are not configured"))
		return
	}
	var request spec.SessionAttachRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.session.Attach(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleSessionStream(w http.ResponseWriter, r *http.Request) {
	if s.session == nil {
		writeError(w, http.StatusNotImplemented, fmt.Errorf("resumable sessions are not configured"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	request, err := sessionAttachRequestFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Subscribe first. An event appended before Attach's atomic capture will be
	// present in its snapshot/replay and skipped from this queue; an event
	// appended afterward has a greater cursor and is delivered live.
	events, cancel := s.ch.SubscribeSession(request.SessionID)
	defer cancel()
	result, err := s.session.Attach(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := writeSSE(w, "session_attached", result); err != nil {
		return
	}
	flusher.Flush()

	boundary := result.Snapshot.Cursor
	ping := time.NewTicker(keepaliveInterval)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-events:
			if event.WorkspaceID != result.WorkspaceID || event.SessionID != result.SessionID {
				continue
			}
			if event.Cursor.Generation != boundary.Generation {
				_ = writeSSE(w, "session_reset", event.Cursor)
				flusher.Flush()
				return
			}
			if event.Cursor.Sequence <= boundary.Sequence {
				continue
			}
			if event.Cursor.Sequence != boundary.Sequence+1 {
				_ = writeSSE(w, "session_gap", event.Cursor)
				flusher.Flush()
				return
			}
			if err := writeSSE(w, "session_event", event); err != nil {
				return
			}
			flusher.Flush()
			boundary = event.Cursor
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func sessionAttachRequestFromQuery(r *http.Request) (spec.SessionAttachRequest, error) {
	query := r.URL.Query()
	request := spec.NewSessionAttachRequest(query.Get("session_id"), query.Get("client_id"))
	request.WorkspaceID = query.Get("workspace_id")
	if value := query.Get("protocol_version"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return spec.SessionAttachRequest{}, fmt.Errorf("invalid protocol_version: %w", err)
		}
		request.ProtocolVersion = uint32(parsed)
	}
	if value := query.Get("schema_revision"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return spec.SessionAttachRequest{}, fmt.Errorf("invalid schema_revision: %w", err)
		}
		request.SchemaRevision = uint32(parsed)
	}
	if capabilities, present := query["capability"]; present {
		request.Capabilities = make([]spec.SessionCapability, len(capabilities))
		for i, capability := range capabilities {
			request.Capabilities[i] = spec.SessionCapability(capability)
		}
	}
	generation, sequence := query.Get("generation"), query.Get("sequence")
	if generation != "" || sequence != "" {
		if generation == "" || sequence == "" {
			return spec.SessionAttachRequest{}, fmt.Errorf("generation and sequence must be provided together")
		}
		parsed, err := strconv.ParseUint(sequence, 10, 64)
		if err != nil {
			return spec.SessionAttachRequest{}, fmt.Errorf("invalid sequence: %w", err)
		}
		request.ResumeAfter = &spec.SessionCursor{Generation: generation, Sequence: parsed}
	}
	if err := request.Validate(); err != nil {
		return spec.SessionAttachRequest{}, err
	}
	return request, nil
}

func writeSSE(w http.ResponseWriter, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}

type renameSessionRequest struct {
	Title string `json:"title"`
}

// handleRenameSession sets a thread's display title (the web transport's
// equivalent of the messaging-spec "rename" control kind; the Aether transport
// handles the control part itself). The title MUST be non-empty per the spec.
func (s *Server) handleRenameSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req renameSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("title must be non-empty"))
		return
	}
	if err := s.sessions.Rename(id, title); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
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

	_, _ = fmt.Fprint(w, ": connected\n\n")
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
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
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
