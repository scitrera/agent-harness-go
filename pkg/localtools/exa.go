// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ExaClient struct {
	endpoint string
	auth     string
	client   *http.Client
}

type ExaSearchResult struct {
	Body string
}

// NewExaClient builds the Exa web_search client. By default the auth value must
// be a sidecar placeholder (the sidecar rewrites it with the real key);
// allowDirect permits a real EXA_API_KEY for sidecar-less testing, in which case
// requests hit Exa directly.
func NewExaClient(endpoint string, auth string, allowDirect bool, client *http.Client) (*ExaClient, error) {
	if !allowDirect && looksLikeRealExternalKey(auth) {
		return nil, ErrExternalAPIKey
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &ExaClient{endpoint: endpoint, auth: auth, client: client}, nil
}

func (c *ExaClient) Search(ctx context.Context, query string) (ExaSearchResult, error) {
	body, err := json.Marshal(struct {
		Query string `json:"query"`
	}{Query: query})
	if err != nil {
		return ExaSearchResult{}, fmt.Errorf("marshal exa request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return ExaSearchResult{}, fmt.Errorf("create exa request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	// x-api-key is Exa's native header (used on direct calls); Authorization is
	// kept for the sidecar rewrite contract. Both carry the same value.
	req.Header.Set("x-api-key", c.auth)
	req.Header.Set("authorization", "Bearer "+c.auth)

	resp, err := c.client.Do(req)
	if err != nil {
		return ExaSearchResult{}, fmt.Errorf("send exa request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return ExaSearchResult{}, fmt.Errorf("read exa response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ExaSearchResult{}, fmt.Errorf("exa status %d: %s", resp.StatusCode, string(data))
	}
	return ExaSearchResult{Body: string(data)}, nil
}

func looksLikeRealExternalKey(value string) bool {
	v := strings.TrimSpace(value)
	if v == "" {
		return false
	}
	lower := strings.ToLower(v)
	if strings.Contains(lower, "placeholder") || strings.Contains(lower, "rewrite") || strings.Contains(lower, "fake") {
		return false
	}
	return strings.HasPrefix(v, "sk-") || strings.HasPrefix(lower, "exa_")
}
