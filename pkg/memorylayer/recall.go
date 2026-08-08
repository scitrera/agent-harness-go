package memorylayer

import (
	"context"
	"net/http"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// Recaller is the semantic-memory half of the MemoryLayer integration: it
// searches the memories MemoryLayer distilled from past conversations, so a turn
// can be given what the agent already knows without that content sitting in the
// thread's transcript.
//
// Memories are not written here. MemoryLayer decomposes stored chat threads into
// memories on its own schedule, and Store already persists the transcript — so
// the write path is "save the conversation", and extraction is the server's job.
// See AppendThreadMessages for why its half of the seam is a no-op.
type Recaller struct {
	client *client
}

// NewRecaller builds a recaller against the same MemoryLayer as a Store.
func NewRecaller(cfg Config) (*Recaller, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Recaller{client: c}, nil
}

// memoryRecord is the subset of MemoryLayer's memory shape the harness surfaces.
type memoryRecord struct {
	ID             string         `json:"id"`
	Content        string         `json:"content"`
	Type           string         `json:"type"`
	Subtype        string         `json:"subtype"`
	Tags           []string       `json:"tags"`
	Metadata       map[string]any `json:"metadata"`
	RelevanceScore float64        `json:"relevance_score"`
	BoostedScore   float64        `json:"boosted_score"`
	CreatedAt      string         `json:"created_at"`
}

type recallResult struct {
	Memories []memoryRecord `json:"memories"`
}

// Recall returns memories relevant to query. An empty query is not an error —
// it asks for the most relevant memories with nothing to match on, which the
// server answers with recency — but it is rarely useful, so callers that want
// query-driven recall must pass the turn's input.
//
// auth is accepted to satisfy the MemoryService seam and ignored: the OSS
// distribution has no authority-grant issuer, so there is no on-behalf-of
// context to forward (see D7 in the design doc).
func (r *Recaller) Recall(ctx context.Context, _ tools.MemoryAuthority, workspace, query string, limit int) ([]tools.MemoryHit, error) {
	if limit <= 0 {
		limit = 10
	}
	body := map[string]any{
		"query": query,
		"limit": limit,
	}
	if ws := firstNonEmpty(workspace, r.client.workspace); ws != "" {
		body["workspace_id"] = ws
	}
	var out recallResult
	if err := r.client.do(ctx, http.MethodPost, "/v1/memories/recall", nil, body, &out); err != nil {
		return nil, err
	}
	hits := make([]tools.MemoryHit, 0, len(out.Memories))
	for _, memory := range out.Memories {
		if strings.TrimSpace(memory.Content) == "" {
			continue
		}
		hits = append(hits, tools.MemoryHit{
			ID:      memory.ID,
			Content: memory.Content,
			Score:   scoreOf(memory),
			Type:    memory.Type,
			Subtype: memory.Subtype,
			Tags:    memory.Tags,
		})
	}
	return hits, nil
}

// AppendThreadMessages is a deliberate no-op.
//
// The seam exists because a distribution may commit the turn to its memory
// backend separately from persisting the transcript — sahara does, because its
// history store is local and MemoryLayer only ever sees the auto-commit. Here
// the history store IS MemoryLayer, so the transcript has already been written
// by the time this would run; doing it again would duplicate every message.
func (r *Recaller) AppendThreadMessages(_ context.Context, _ tools.MemoryAuthority, _, _, _ string, _ []protocol.ChatMessage) error {
	return nil
}

// scoreOf prefers the boosted score (what the server actually ranked on) and
// falls back to raw relevance.
func scoreOf(memory memoryRecord) float64 {
	if memory.BoostedScore != 0 {
		return memory.BoostedScore
	}
	return memory.RelevanceScore
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
