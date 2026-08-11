package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

// MemoryHit is a single recalled memory.
type MemoryHit struct {
	ID      string  `json:"id"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`

	// Detail fields — populated by memory_get (fetch-by-id) to give the model
	// full context + provenance; memory_search leaves them empty so scan results
	// stay lean (omitempty drops them). Document provenance (source filename,
	// document id, page) lives in Metadata/Tags for ingested memories.
	Type           string         `json:"type,omitempty"`
	Subtype        string         `json:"subtype,omitempty"`
	Tags           []string       `json:"tags,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	Abstract       string         `json:"abstract,omitempty"`
	SourceMemoryID string         `json:"source_memory_id,omitempty"`
	CreatedAt      string         `json:"created_at,omitempty"`
}

// MemoryAuthority is the per-turn OBO grant for MemoryLayer calls. A zero value
// means "use the client's default authority" (the configured fallback grant).
type MemoryAuthority struct {
	GrantID     string
	SubjectType string
	SubjectID   string
}

// memoryAuthorityKey is the unexported context key carrying the per-turn
// MemoryAuthority. Auth is a request-scoped, cross-cutting concern that transits
// intermediary layers (e.g. a caching history store) which don't themselves care
// about it, so a typed context value — set once by the runner, read at the
// MemoryLayer boundary — is the right seam, rather than threading an auth
// parameter through interfaces that mostly ignore it.
type memoryAuthorityKey struct{}

// WithMemoryAuthority returns a context carrying the per-turn MemoryAuthority.
// The runner sets this before a history load so an authority-aware store can
// apply the user's OBO grant; stores that don't need it simply ignore it.
func WithMemoryAuthority(ctx context.Context, auth MemoryAuthority) context.Context {
	return context.WithValue(ctx, memoryAuthorityKey{}, auth)
}

// MemoryAuthorityFrom returns the MemoryAuthority carried on ctx, and whether one
// was present. Absence yields the zero value (no OBO → client/default behavior).
func MemoryAuthorityFrom(ctx context.Context) (MemoryAuthority, bool) {
	auth, ok := ctx.Value(memoryAuthorityKey{}).(MemoryAuthority)
	return auth, ok
}

// MemoryRecaller is the read surface of MemoryLayer the harness exposes to the
// model as on-demand tools (semantic recall + fetch-by-id). Implemented by the
// MemoryLayer SDK adapter in internal/memory. Workspace + OBO authority are
// passed per call (the turn's chat workspace and grant) rather than baked into
// the client.
type MemoryRecaller interface {
	Recall(ctx context.Context, auth MemoryAuthority, workspace, query string, limit int) ([]MemoryHit, error)
	GetMemory(ctx context.Context, auth MemoryAuthority, id string) (MemoryHit, error)
}

// RegisterMemory registers the memory_search and memory_get tools backed by the
// recaller, with descriptors so they are exposed via the provider tool-API.
func RegisterMemory(reg *Registry, recaller MemoryRecaller) error {
	if recaller == nil {
		return fmt.Errorf("%w: memory recaller required", ErrInvalidTool)
	}
	if err := reg.Register("memory_search", HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		return memorySearch(ctx, recaller, req)
	})); err != nil {
		return err
	}
	if err := reg.Register("memory_get", HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		return memoryGet(ctx, recaller, req)
	})); err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        "memory_search",
		Description: "Semantically search durable memory for relevant facts, decisions, and context from past turns.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"What to recall"},"limit":{"type":"integer","description":"Max results (default 10)"}},"required":["query"]}`),
		Concurrency: ConcurrencyParallelSafe,
	})
	reg.Describe(Descriptor{
		Name:        "memory_get",
		Description: "Fetch a memory by id (from memory_search results) with full detail: content, type/subtype, tags, timestamps, and metadata — including source provenance (e.g. source_filename, source_document_id, page_number) for memories ingested from documents.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`),
		Concurrency: ConcurrencyParallelSafe,
	})
	return nil
}

func memorySearch(ctx context.Context, recaller MemoryRecaller, req Request) (Result, error) {
	var args struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	hits, err := recaller.Recall(ctx, req.Authority, req.Addr.WorkspaceID, args.Query, args.Limit)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, struct {
		Results []MemoryHit `json:"results"`
	}{Results: hits})
}

func memoryGet(ctx context.Context, recaller MemoryRecaller, req Request) (Result, error) {
	var args struct {
		ID string `json:"id"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	hit, err := recaller.GetMemory(ctx, req.Authority, args.ID)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, hit)
}
