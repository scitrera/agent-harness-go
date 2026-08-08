// Package memorylayer stores chat threads and transcripts in MemoryLayer
// instead of on local disk, so every process that shares a MemoryLayer sees the
// same conversation — the agent worker and any number of attached frontends.
//
// It speaks MemoryLayer's REST API directly rather than through the Go SDK:
// that SDK is not published as a Go module, and depending on it would mean an
// absolute-path replace directive in this repo. The messaging spec anticipates
// exactly this — its MemoryLayer codec produces and consumes generic maps and
// carries no SDK dependency — so the conversion stays lossless either way.
package memorylayer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTimeout = 15 * time.Second
	// defaultWindow bounds how many of a thread's most recent messages are
	// loaded. The context assembler caps history well below this; the window
	// only has to cover a turn's usable context.
	defaultWindow = 200
	// defaultOwnership stores threads under the workspace rather than folding
	// them into MemoryLayer's user-chat home. The OSS harness has no
	// authenticated user identity to claim (a user-owned thread that names a
	// user without one is rejected), so workspace ownership is the honest
	// mapping.
	defaultOwnership = "workspace"
)

// Config configures the MemoryLayer-backed store.
type Config struct {
	// BaseURL is the MemoryLayer server root, e.g. http://127.0.0.1:61001.
	BaseURL string
	// APIKey is sent as a bearer token when set. A local OSS MemoryLayer needs
	// none.
	APIKey string
	// Workspace scopes threads and messages.
	Workspace string
	// Ownership is "workspace" (default) or "user".
	Ownership string
	// Window caps messages loaded per thread.
	Window int
	// HTTPClient overrides the default client (timeouts, transport, tracing).
	HTTPClient *http.Client
}

type client struct {
	baseURL   *url.URL
	apiKey    string
	workspace string
	ownership string
	window    int
	http      *http.Client
}

func newClient(cfg Config) (*client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("memorylayer: base url required")
	}
	parsed, err := url.ParseRequestURI(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("memorylayer: base url: %w", err)
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	ownership := cfg.Ownership
	if ownership == "" {
		ownership = defaultOwnership
	}
	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}
	return &client{
		baseURL:   parsed,
		apiKey:    cfg.APIKey,
		workspace: cfg.Workspace,
		ownership: ownership,
		window:    window,
		http:      httpClient,
	}, nil
}

// do issues a request and decodes a JSON response into out (nil to discard).
func (c *client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	if query != nil {
		endpoint.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("memorylayer: encode %s: %w", path, err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return fmt.Errorf("memorylayer: request %s: %w", path, err)
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("memorylayer: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("memorylayer: %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(out); err != nil {
		return fmt.Errorf("memorylayer: decode %s: %w", path, err)
	}
	return nil
}

func (c *client) workspaceQuery() url.Values {
	q := url.Values{}
	if c.workspace != "" {
		q.Set("workspace_id", c.workspace)
	}
	return q
}

// thread is MemoryLayer's chat-thread record, reduced to the fields the harness
// uses for its thread switcher.
type thread struct {
	ID        string  `json:"id"`
	Title     *string `json:"title"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

// The server wraps single-object responses ({"thread": …}) and lists
// ({"threads": […]}, {"messages": […]}).
type threadEnvelope struct {
	Thread thread `json:"thread"`
}

type threadListEnvelope struct {
	Threads []thread `json:"threads"`
}

type messageListEnvelope struct {
	Messages []map[string]any `json:"messages"`
}

// ensureWorkspace creates the configured workspace when the server does not
// have it.
//
// Servers from before memorylayer oss 0e01496 resolve the workspace to
// auto-create from the request body only, never the query string — so a write
// that names its workspace as a query parameter (which is how the chat routes
// take it) lands in a workspace that was never created, and the
// chat_threads.workspace_id foreign key fails as an opaque
// 500 "Failed to append messages". MemoryLayer ships `_default`, not `default`,
// so a stock configuration hits it on every write.
//
// A fixed server makes this redundant but harmless; it stays so the harness
// works against already-deployed ones.
func (c *client) ensureWorkspace(ctx context.Context) error {
	if c.workspace == "" {
		return nil
	}
	err := c.do(ctx, http.MethodGet, "/v1/workspaces/"+url.PathEscape(c.workspace), nil, nil, nil)
	if err == nil {
		return nil
	}
	if !errors.Is(err, errNotFound) {
		return err
	}
	createErr := c.do(ctx, http.MethodPost, "/v1/workspaces", nil,
		map[string]any{"id": c.workspace, "name": c.workspace}, nil)
	if createErr != nil {
		// A concurrent process may have won the race; treat an existing
		// workspace as success.
		if verifyErr := c.do(ctx, http.MethodGet, "/v1/workspaces/"+url.PathEscape(c.workspace), nil, nil, nil); verifyErr == nil {
			return nil
		}
		return createErr
	}
	return nil
}

// createThread makes a new thread. The server mints the id and IGNORES one
// supplied by the client, so the caller must adopt the returned id rather than
// assume its own — passing one silently creates a thread under a different id.
func (c *client) createThread(ctx context.Context, title string) (thread, error) {
	body := map[string]any{"ownership": c.ownership}
	if c.workspace != "" {
		body["workspace_id"] = c.workspace
	}
	if title != "" {
		body["title"] = title
	}
	var out threadEnvelope
	if err := c.do(ctx, http.MethodPost, "/v1/threads", c.workspaceQuery(), body, &out); err != nil {
		return thread{}, err
	}
	return out.Thread, nil
}

func (c *client) listThreads(ctx context.Context, limit int) ([]thread, error) {
	q := c.workspaceQuery()
	q.Set("limit", strconv.Itoa(limit))
	// No ownership filter: appending to an unknown thread id auto-creates the
	// thread as user-owned regardless of what the append asks for, so filtering
	// on workspace ownership hides exactly the conversations the agent creates.
	var out threadListEnvelope
	if err := c.do(ctx, http.MethodGet, "/v1/threads", q, nil, &out); err != nil {
		return nil, err
	}
	return out.Threads, nil
}

func (c *client) updateThread(ctx context.Context, id, title string) error {
	return c.do(ctx, http.MethodPut, "/v1/threads/"+url.PathEscape(id), c.workspaceQuery(),
		map[string]any{"title": title}, nil)
}

func (c *client) deleteThread(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/v1/threads/"+url.PathEscape(id), c.workspaceQuery(), nil, nil)
	if err == errNotFound {
		return nil
	}
	return err
}

func (c *client) appendMessages(ctx context.Context, threadID string, payloads []map[string]any) error {
	body := map[string]any{"messages": payloads, "ownership": c.ownership}
	if c.workspace != "" {
		body["workspace_id"] = c.workspace
	}
	return c.do(ctx, http.MethodPost,
		"/v1/threads/"+url.PathEscape(threadID)+"/messages", c.workspaceQuery(), body, nil)
}

// deleteMessage removes one message. Clearing a transcript goes message by
// message because there is no bulk delete and dropping the thread would remove
// it from the switcher.
func (c *client) deleteMessage(ctx context.Context, threadID, messageID string) error {
	err := c.do(ctx, http.MethodDelete,
		"/v1/threads/"+url.PathEscape(threadID)+"/messages/"+url.PathEscape(messageID),
		c.workspaceQuery(), nil, nil)
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}

// messageIDs returns the server-side ids of a thread's messages.
func (c *client) messageIDs(ctx context.Context, threadID string) ([]string, error) {
	raw, err := c.getMessages(ctx, threadID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(raw))
	for _, item := range raw {
		if id, ok := item["id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (c *client) getMessages(ctx context.Context, threadID string) ([]map[string]any, error) {
	q := c.workspaceQuery()
	q.Set("limit", strconv.Itoa(c.window))
	q.Set("order", "asc")
	var out messageListEnvelope
	if err := c.do(ctx, http.MethodGet,
		"/v1/threads/"+url.PathEscape(threadID)+"/messages", q, nil, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}
